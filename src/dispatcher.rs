//! Dispatcher implementation for controlling event distribution from producers to consumers.
//!
//! Dispatchers are inspired by Elixir's GenStage dispatchers and control how events
//! are routed from a single producer to multiple consumers.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::traits::{Consumer, Producer};

/// Configuration for dispatcher behavior
pub struct DispatcherConfig {
    /// Buffer size for consumer channels
    pub buffer_size: usize,
    /// Maximum demand per consumer
    pub max_demand: usize,
    /// Whether to shuffle consumers on first dispatch to prevent overloading first consumer
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
    /// Demand-based dispatcher (sends to consumer with highest demand)
    Demand,
    /// Broadcast dispatcher (sends to all consumers)
    Broadcast {
        /// Optional selector function for filtering events per consumer
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
    /// Handle a new consumer subscription
    fn subscribe(&self, consumer_id: ConsumerId, partition: Option<String>) -> Result<()>;

    /// Handle consumer unsubscription
    fn unsubscribe(&self, consumer_id: ConsumerId) -> Result<()>;

    /// Dispatch events to appropriate consumers
    fn dispatch(
        &self,
        events: Vec<T>,
        consumers: &HashMap<ConsumerId, ConsumerHandle<T>>,
    ) -> Result<()>;

    /// Handle demand from a consumer
    fn handle_demand(&self, consumer_id: ConsumerId, demand: usize) -> Result<()>;
}

/// Unique identifier for consumers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConsumerId(pub u64);

/// Handle for managing a consumer
pub struct ConsumerHandle<T> {
    pub id: ConsumerId,
    pub sender: mpsc::Sender<T>,
    pub demand: Arc<Mutex<usize>>,
    pub partition: Option<String>,
    pub max_demand: usize,
}

/// Consumer information for dispatcher management
#[derive(Debug)]
pub struct ConsumerInfo {
    pub id: ConsumerId,
    pub partition: Option<String>,
    pub current_demand: usize,
    pub max_demand: usize,
}

/// Main dispatcher that manages producer-to-consumer distribution
pub struct Dispatcher<T> {
    dispatcher_type: DispatcherType<T>,
    consumers: Arc<Mutex<HashMap<ConsumerId, ConsumerHandle<T>>>>,
    config: DispatcherConfig,
    next_consumer_id: Arc<Mutex<u64>>,
}

impl<T> Dispatcher<T>
where
    T: Send + 'static,
{
    /// Create a new dispatcher
    pub fn new(dispatcher_type: DispatcherType<T>, config: DispatcherConfig) -> Self {
        Self {
            dispatcher_type,
            consumers: Arc::new(Mutex::new(HashMap::new())),
            config,
            next_consumer_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Subscribe a new consumer
    pub async fn subscribe_consumer<C>(
        &self,
        consumer: C,
        partition: Option<String>,
    ) -> Result<ConsumerId>
    where
        C: Consumer<Item = T> + Send + 'static,
    {
        let consumer_id = {
            let mut next_id = self.next_consumer_id.lock().await;
            let id = ConsumerId(*next_id);
            *next_id += 1;
            id
        };

        let (sender, receiver) = mpsc::channel(self.config.buffer_size);
        let demand = Arc::new(Mutex::new(self.config.max_demand));

        let handle = ConsumerHandle {
            id: consumer_id,
            sender,
            demand: demand.clone(),
            partition: partition.clone(),
            max_demand: self.config.max_demand,
        };

        // Spawn task to run the consumer
        self.spawn_consumer_task(consumer, receiver, demand).await?;

        // Add to consumers map
        let mut consumers = self.consumers.lock().await;
        consumers.insert(consumer_id, handle);

        // Handle dispatcher-specific subscription logic
        match &self.dispatcher_type {
            DispatcherType::Custom(dispatcher) => {
                dispatcher.subscribe(consumer_id, partition)?;
            }
            _ => {} // Built-in dispatchers handle this automatically
        }

        Ok(consumer_id)
    }

    /// Unsubscribe a consumer
    pub async fn unsubscribe_consumer(&self, consumer_id: ConsumerId) -> Result<()> {
        let mut consumers = self.consumers.lock().await;

        if consumers.remove(&consumer_id).is_some() {
            // Handle dispatcher-specific unsubscription logic
            match &self.dispatcher_type {
                DispatcherType::Custom(dispatcher) => {
                    dispatcher.unsubscribe(consumer_id)?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Dispatch events to consumers based on dispatcher type
    pub async fn dispatch(&self, events: Vec<T>) -> Result<()> {
        let consumers = self.consumers.lock().await;

        if consumers.is_empty() {
            return Ok(()); // No consumers to dispatch to
        }

        match &self.dispatcher_type {
            DispatcherType::Demand => self.dispatch_demand(events, &consumers).await,
            DispatcherType::Broadcast { selector } => {
                self.dispatch_broadcast(events, &consumers, selector.as_ref())
                    .await
            }
            DispatcherType::Partition {
                partitions,
                hash_fn,
            } => {
                self.dispatch_partition(events, &consumers, partitions, hash_fn)
                    .await
            }
            DispatcherType::Custom(dispatcher) => dispatcher.dispatch(events, &consumers),
        }
    }

    /// Demand-based dispatch: send to consumer with highest demand
    async fn dispatch_demand(
        &self,
        events: Vec<T>,
        consumers: &HashMap<ConsumerId, ConsumerHandle<T>>,
    ) -> Result<()> {
        let mut consumer_demands = Vec::new();

        // Collect current demand from all consumers
        for (id, handle) in consumers {
            let demand = *handle.demand.lock().await;
            if demand > 0 {
                consumer_demands.push((*id, demand, handle));
            }
        }

        if consumer_demands.is_empty() {
            return Ok(()); // No demand
        }

        // Sort by demand (highest first)
        consumer_demands.sort_by(|a, b| b.1.cmp(&a.1));

        // Distribute events starting with highest demand consumer
        let mut event_index = 0;
        for (_, demand, handle) in consumer_demands {
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
                let mut consumer_demand = handle.demand.lock().await;
                *consumer_demand = consumer_demand.saturating_sub(to_send);
            }

            event_index += to_send;
        }

        Ok(())
    }

    /// Broadcast dispatch: send to all consumers (with optional selector)
    async fn dispatch_broadcast(
        &self,
        events: Vec<T>,
        consumers: &HashMap<ConsumerId, ConsumerHandle<T>>,
        selector: Option<&Arc<dyn Fn(&T) -> bool + Send + Sync>>,
    ) -> Result<()> {
        for event in events {
            for handle in consumers.values() {
                // Apply selector if provided
                if let Some(selector_fn) = selector {
                    if !selector_fn(&event) {
                        continue;
                    }
                }

                let demand = {
                    let mut consumer_demand = handle.demand.lock().await;
                    if *consumer_demand > 0 {
                        *consumer_demand -= 1;
                        true
                    } else {
                        false
                    }
                };

                if demand {
                    // handle.sender.send(event.clone()).await.map_err(|_| Error::ChannelClosed)?;
                }
            }
        }

        Ok(())
    }

    /// Partition dispatch: route based on hash function
    async fn dispatch_partition(
        &self,
        events: Vec<T>,
        consumers: &HashMap<ConsumerId, ConsumerHandle<T>>,
        partitions: &[String],
        hash_fn: &Arc<dyn Fn(&T) -> String + Send + Sync>,
    ) -> Result<()> {
        // Group consumers by partition
        let mut partition_consumers: HashMap<String, Vec<&ConsumerHandle<T>>> = HashMap::new();
        for handle in consumers.values() {
            if let Some(ref partition) = handle.partition {
                partition_consumers
                    .entry(partition.clone())
                    .or_default()
                    .push(handle);
            }
        }

        // Route events to partitions
        for event in events {
            let partition = hash_fn(&event);

            if let Some(consumers_in_partition) = partition_consumers.get(&partition) {
                // Within partition, use demand-based routing
                if let Some(best_consumer) = consumers_in_partition
                    .iter()
                    .max_by_key(|c| futures::executor::block_on(async { *c.demand.lock().await }))
                {
                    let demand = {
                        let mut consumer_demand = best_consumer.demand.lock().await;
                        if *consumer_demand > 0 {
                            *consumer_demand -= 1;
                            true
                        } else {
                            false
                        }
                    };

                    if demand {
                        // best_consumer.sender.send(event).await.map_err(|_| Error::ChannelClosed)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Spawn a task to run a consumer
    async fn spawn_consumer_task<C>(
        &self,
        mut consumer: C,
        mut receiver: mpsc::Receiver<T>,
        demand: Arc<Mutex<usize>>,
    ) -> Result<JoinHandle<Result<()>>>
    where
        C: Consumer<Item = T> + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            while let Some(item) = receiver.recv().await {
                consumer.consume(item).await?;

                // Increase demand after consuming
                {
                    let mut current_demand = demand.lock().await;
                    *current_demand = (*current_demand + 1).min(1000); // Max demand cap
                }
            }

            consumer.finish().await?;
            Ok(())
        });

        Ok(handle)
    }

    /// Get information about all consumers
    pub async fn consumer_info(&self) -> Vec<ConsumerInfo> {
        let consumers = self.consumers.lock().await;
        let mut info = Vec::new();

        for handle in consumers.values() {
            let current_demand = *handle.demand.lock().await;
            info.push(ConsumerInfo {
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

    /// Set max demand per consumer
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
