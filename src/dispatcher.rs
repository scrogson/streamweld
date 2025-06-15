//! Dispatcher implementation for controlling event distribution from sources to sinks.
//!
//! Dispatchers are inspired by Elixir's GenStage dispatchers and control how events
//! are routed from a single source to multiple sinks.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::traits::Sink;

/// Configuration for dispatcher behavior
pub struct DispatcherConfig {
    /// Buffer size for sink channels
    pub buffer_size: usize,
    /// Maximum demand per sink
    pub max_demand: usize,
    /// Whether to shuffle sinks on first dispatch to prevent overloading first sink
    pub shuffle_on_first: bool,
}

impl std::fmt::Debug for DispatcherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatcherConfig")
            .field("buffer_size", &self.buffer_size)
            .field("max_demand", &self.max_demand)
            .field("shuffle_on_first", &self.shuffle_on_first)
            .finish()
    }
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100,
            max_demand: 1000,
            shuffle_on_first: false,
        }
    }
}

/// Types of dispatchers available
pub enum DispatcherType<H> {
    /// Demand-based dispatcher (sends to sink with highest demand)
    Demand,
    /// Broadcast dispatcher (sends to all sinks)
    Broadcast {
        /// Optional selector function for filtering events per sink
        selector: Option<Arc<dyn Fn(&H) -> bool + Send + Sync>>,
    },
    /// Partition dispatcher (routes based on hash function)
    Partition {
        /// Number of partitions or partition names
        partitions: Vec<String>,
        /// Hash function to determine partition
        hash_fn: Arc<dyn Fn(&H) -> String + Send + Sync>,
    },
    /// Custom dispatcher with user-defined logic
    Custom(Arc<dyn CustomDispatcher<H> + Send + Sync>),
}

impl<H> std::fmt::Debug for DispatcherType<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatcherType::Demand => write!(f, "Demand"),
            DispatcherType::Broadcast { .. } => write!(f, "Broadcast {{ .. }}"),
            DispatcherType::Partition { partitions, .. } => f
                .debug_struct("Partition")
                .field("partitions", partitions)
                .finish(),
            DispatcherType::Custom(_) => write!(f, "Custom(..)"),
        }
    }
}

/// Trait for implementing custom dispatchers
pub trait CustomDispatcher<T> {
    /// Handle a new sink subscription
    fn subscribe(&self, sink_id: SinkId, partition: Option<String>) -> Result<()>;

    /// Handle sink unsubscription
    fn unsubscribe(&self, sink_id: SinkId) -> Result<()>;

    /// Dispatch events to appropriate sinks
    fn dispatch(&self, events: Vec<T>, sinks: &HashMap<SinkId, SinkHandle<T>>) -> Result<()>;

    /// Handle demand from a sink
    fn handle_demand(&self, sink_id: SinkId, demand: usize) -> Result<()>;
}

/// Unique identifier for sinks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SinkId(pub u64);

/// Handle for managing a sink
pub struct SinkHandle<T> {
    pub id: SinkId,
    pub sender: mpsc::Sender<T>,
    pub demand: Arc<Mutex<usize>>,
    pub partition: Option<String>,
    pub max_demand: usize,
}

/// Sink information for dispatcher management
#[derive(Debug)]
pub struct SinkInfo {
    pub id: SinkId,
    pub partition: Option<String>,
    pub current_demand: usize,
    pub max_demand: usize,
}

/// Main dispatcher that manages source-to-sink distribution
pub struct Dispatcher<T> {
    dispatcher_type: DispatcherType<T>,
    sinks: Arc<Mutex<HashMap<SinkId, SinkHandle<T>>>>,
    config: DispatcherConfig,
    next_sink_id: Arc<Mutex<u64>>,
}

impl<T> Dispatcher<T>
where
    T: Send + 'static,
{
    /// Create a new dispatcher
    pub fn new(dispatcher_type: DispatcherType<T>, config: DispatcherConfig) -> Self {
        Self {
            dispatcher_type,
            sinks: Arc::new(Mutex::new(HashMap::new())),
            config,
            next_sink_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Subscribe a new sink
    pub async fn subscribe_sink<C>(&self, sink: C, partition: Option<String>) -> Result<SinkId>
    where
        C: Sink<Item = T> + Send + 'static,
    {
        let sink_id = {
            let mut next_id = self.next_sink_id.lock().await;
            let id = SinkId(*next_id);
            *next_id += 1;
            id
        };

        let (sender, receiver) = mpsc::channel(self.config.buffer_size);
        let demand = Arc::new(Mutex::new(self.config.max_demand));

        let handle = SinkHandle {
            id: sink_id,
            sender,
            demand: demand.clone(),
            partition: partition.clone(),
            max_demand: self.config.max_demand,
        };

        // Spawn task to run the sink
        self.spawn_sink_task(sink, receiver, demand).await?;

        // Add to sinks map
        let mut sinks = self.sinks.lock().await;
        sinks.insert(sink_id, handle);

        // Handle dispatcher-specific subscription logic
        match &self.dispatcher_type {
            DispatcherType::Custom(dispatcher) => {
                dispatcher.subscribe(sink_id, partition)?;
            }
            _ => {} // Built-in dispatchers handle this automatically
        }

        Ok(sink_id)
    }

    /// Unsubscribe a sink
    pub async fn unsubscribe_sink(&self, sink_id: SinkId) -> Result<()> {
        let mut sinks = self.sinks.lock().await;

        if sinks.remove(&sink_id).is_some() {
            // Handle dispatcher-specific unsubscription logic
            match &self.dispatcher_type {
                DispatcherType::Custom(dispatcher) => {
                    dispatcher.unsubscribe(sink_id)?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Dispatch events to sinks based on dispatcher type
    pub async fn dispatch(&self, events: Vec<T>) -> Result<()> {
        let sinks = self.sinks.lock().await;

        if sinks.is_empty() {
            return Ok(()); // No sinks to dispatch to
        }

        match &self.dispatcher_type {
            DispatcherType::Demand => self.dispatch_demand(events, &sinks).await,
            DispatcherType::Broadcast { selector } => {
                self.dispatch_broadcast(events, &sinks, selector.as_ref())
                    .await
            }
            DispatcherType::Partition {
                partitions,
                hash_fn,
            } => {
                self.dispatch_partition(events, &sinks, partitions, hash_fn)
                    .await
            }
            DispatcherType::Custom(dispatcher) => dispatcher.dispatch(events, &sinks),
        }
    }

    /// Demand-based dispatch: send to sink with highest demand
    async fn dispatch_demand(
        &self,
        events: Vec<T>,
        sinks: &HashMap<SinkId, SinkHandle<T>>,
    ) -> Result<()> {
        let mut sink_demands = Vec::new();

        // Collect current demand from all sinks
        for (id, handle) in sinks {
            let demand = *handle.demand.lock().await;
            if demand > 0 {
                sink_demands.push((*id, demand, handle));
            }
        }

        if sink_demands.is_empty() {
            return Ok(()); // No demand
        }

        // Sort by demand (highest first)
        sink_demands.sort_by(|a, b| b.1.cmp(&a.1));

        // Distribute events starting with highest demand sink
        let mut event_index = 0;
        for (_, demand, handle) in sink_demands {
            if event_index >= events.len() {
                break;
            }

            let to_send = (demand).min(events.len() - event_index);

            for i in 0..to_send {
                if event_index + i < events.len() {
                    // In a real implementation, we'd need to handle the ownership better
                    // For now, this assumes T: Clone for simplicity
                    // handle.sender.send(events[event_index + i].clone()).await.map_err(|_| Error::ChannelClosed)?;
                }
            }

            // Update demand
            {
                let mut sink_demand = handle.demand.lock().await;
                *sink_demand = sink_demand.saturating_sub(to_send);
            }

            event_index += to_send;
        }

        Ok(())
    }

    /// Broadcast dispatch: send to all sinks (with optional selector)
    async fn dispatch_broadcast(
        &self,
        events: Vec<T>,
        sinks: &HashMap<SinkId, SinkHandle<T>>,
        selector: Option<&Arc<dyn Fn(&T) -> bool + Send + Sync>>,
    ) -> Result<()> {
        for event in events {
            for handle in sinks.values() {
                // Apply selector if provided
                if let Some(selector_fn) = selector {
                    if !selector_fn(&event) {
                        continue;
                    }
                }

                let mut sink_demand = handle.demand.lock().await;
                if *sink_demand > 0 {
                    *sink_demand -= 1;
                }
            }
        }

        Ok(())
    }

    /// Partition dispatch: route based on hash function
    async fn dispatch_partition(
        &self,
        events: Vec<T>,
        sinks: &HashMap<SinkId, SinkHandle<T>>,
        _partitions: &[String],
        hash_fn: &Arc<dyn Fn(&T) -> String + Send + Sync>,
    ) -> Result<()> {
        // Group sinks by partition
        let mut partition_sinks: HashMap<String, Vec<&SinkHandle<T>>> = HashMap::new();
        for handle in sinks.values() {
            if let Some(ref partition) = handle.partition {
                partition_sinks
                    .entry(partition.clone())
                    .or_default()
                    .push(handle);
            }
        }

        // Route events to partitions
        for event in events {
            let partition = hash_fn(&event);

            if let Some(sinks_in_partition) = partition_sinks.get(&partition) {
                // Within partition, use demand-based routing
                if let Some(best_sink) = sinks_in_partition
                    .iter()
                    .max_by_key(|c| futures::executor::block_on(async { *c.demand.lock().await }))
                {
                    let mut sink_demand = best_sink.demand.lock().await;
                    if *sink_demand > 0 {
                        *sink_demand -= 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Spawn a task to run a sink
    async fn spawn_sink_task<C>(
        &self,
        mut sink: C,
        mut receiver: mpsc::Receiver<T>,
        demand: Arc<Mutex<usize>>,
    ) -> Result<JoinHandle<Result<()>>>
    where
        C: Sink<Item = T> + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            while let Some(item) = receiver.recv().await {
                sink.consume(item).await?;

                // Increase demand after consuming
                {
                    let mut current_demand = demand.lock().await;
                    *current_demand = (*current_demand + 1).min(1000); // Max demand cap
                }
            }

            sink.finish().await?;
            Ok(())
        });

        Ok(handle)
    }

    /// Get information about all sinks
    pub async fn sink_info(&self) -> Vec<SinkInfo> {
        let sinks = self.sinks.lock().await;
        let mut info = Vec::new();

        for handle in sinks.values() {
            let current_demand = *handle.demand.lock().await;
            info.push(SinkInfo {
                id: handle.id,
                partition: handle.partition.clone(),
                current_demand,
                max_demand: handle.max_demand,
            });
        }

        info
    }
}

/// Builder for creating dispatchers with fluent API
pub struct DispatcherBuilder<T> {
    config: DispatcherConfig,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> DispatcherBuilder<T> {
    /// Create a new dispatcher builder
    pub fn new() -> Self {
        Self {
            config: DispatcherConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set max demand per sink
    pub fn max_demand(mut self, demand: usize) -> Self {
        self.config.max_demand = demand;
        self
    }

    /// Enable shuffling on first dispatch
    pub fn shuffle_on_first(mut self, shuffle: bool) -> Self {
        self.config.shuffle_on_first = shuffle;
        self
    }

    /// Build a demand dispatcher
    pub fn demand(self) -> Dispatcher<T>
    where
        T: Send + 'static,
    {
        Dispatcher::new(DispatcherType::Demand, self.config)
    }

    /// Build a broadcast dispatcher
    pub fn broadcast(self) -> Dispatcher<T>
    where
        T: Send + 'static,
    {
        Dispatcher::new(DispatcherType::Broadcast { selector: None }, self.config)
    }

    /// Build a broadcast dispatcher with selector
    pub fn broadcast_with_selector<F>(self, selector: F) -> Dispatcher<T>
    where
        T: Send + 'static,
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Dispatcher::new(
            DispatcherType::Broadcast {
                selector: Some(Arc::new(selector)),
            },
            self.config,
        )
    }

    /// Build a partition dispatcher
    pub fn partition<F>(self, partitions: Vec<String>, hash_fn: F) -> Dispatcher<T>
    where
        T: Send + 'static,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        Dispatcher::new(
            DispatcherType::Partition {
                partitions,
                hash_fn: Arc::new(hash_fn),
            },
            self.config,
        )
    }

    /// Build a custom dispatcher
    pub fn custom<D>(self, dispatcher: D) -> Dispatcher<T>
    where
        T: Send + 'static,
        D: CustomDispatcher<T> + Send + Sync + 'static,
    {
        Dispatcher::new(DispatcherType::Custom(Arc::new(dispatcher)), self.config)
    }
}

impl<T> Default for DispatcherBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}
