//! Integration between pipelines and dispatchers for multi-sink patterns.

use std::sync::Arc;

use crate::core::{Result, Sink};
use crate::utils::dispatcher::{Dispatcher, DispatcherBuilder, SinkId};

/// A sink that forwards items to a dispatcher
pub struct DispatcherSink<T> {
    dispatcher: Arc<Dispatcher<T>>,
    batch_size: usize,
    batch: Vec<T>,
}

impl<T> DispatcherSink<T>
where
    T: Send + 'static,
{
    /// Create a new dispatcher sink
    pub fn new(dispatcher: Arc<Dispatcher<T>>, batch_size: usize) -> Self {
        Self {
            dispatcher,
            batch_size,
            batch: Vec::new(),
        }
    }

    /// Flush the current batch to the dispatcher
    async fn flush_batch(&mut self) -> Result<()> {
        if !self.batch.is_empty() {
            let batch = std::mem::take(&mut self.batch);
            self.dispatcher.dispatch(batch).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: Send + 'static> Sink for DispatcherSink<T> {
    type Item = T;

    async fn write(&mut self, item: Self::Item) -> Result<()> {
        self.batch.push(item);

        if self.batch.len() >= self.batch_size {
            self.flush_batch().await?;
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.flush_batch().await
    }
}

/// A simple dispatched pipeline that wraps a dispatcher
pub struct DispatchedPipeline<T> {
    dispatcher: Arc<Dispatcher<T>>,
}

impl<T> DispatchedPipeline<T>
where
    T: Send + 'static,
{
    /// Create a new dispatched pipeline
    pub fn new(dispatcher: Arc<Dispatcher<T>>) -> Self {
        Self { dispatcher }
    }

    /// Subscribe a sink to the dispatcher
    pub async fn subscribe_sink<C>(&self, sink: C, partition: Option<String>) -> Result<SinkId>
    where
        C: Sink<Item = T> + Send + 'static,
    {
        self.dispatcher.subscribe_sink(sink, partition).await
    }

    /// Unsubscribe a sink from the dispatcher
    pub async fn unsubscribe_sink(&self, sink_id: SinkId) -> Result<()> {
        self.dispatcher.unsubscribe_sink(sink_id).await
    }

    /// Get the dispatcher for direct access
    pub fn dispatcher(&self) -> &Arc<Dispatcher<T>> {
        &self.dispatcher
    }

    /// Create a dispatcher sink that can be used with regular pipelines
    pub fn create_sink(&self, batch_size: usize) -> DispatcherSink<T> {
        DispatcherSink::new(self.dispatcher.clone(), batch_size)
    }
}

/// Builder for creating dispatched pipelines
pub struct DispatchedPipelineBuilder<T> {
    dispatcher_builder: DispatcherBuilder<T>,
}

impl<T> DispatchedPipelineBuilder<T> {
    /// Create a new dispatched pipeline builder
    pub fn new() -> Self {
        Self {
            dispatcher_builder: DispatcherBuilder::new(),
        }
    }

    /// Set buffer size for the dispatcher
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.dispatcher_builder = self.dispatcher_builder.buffer_size(size);
        self
    }

    /// Set max demand per sink
    pub fn max_demand(mut self, demand: usize) -> Self {
        self.dispatcher_builder = self.dispatcher_builder.max_demand(demand);
        self
    }

    /// Build a demand-based dispatched pipeline
    pub fn demand(self) -> DispatchedPipeline<T>
    where
        T: Send + 'static,
    {
        let dispatcher = Arc::new(self.dispatcher_builder.demand());
        DispatchedPipeline::new(dispatcher)
    }

    /// Build a broadcast dispatched pipeline
    pub fn broadcast(self) -> DispatchedPipeline<T>
    where
        T: Send + 'static,
    {
        let dispatcher = Arc::new(self.dispatcher_builder.broadcast());
        DispatchedPipeline::new(dispatcher)
    }

    /// Build a partition-based dispatched pipeline
    pub fn partition<F>(self, partitions: Vec<String>, hash_fn: F) -> DispatchedPipeline<T>
    where
        T: Send + 'static,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        let dispatcher = Arc::new(self.dispatcher_builder.partition(partitions, hash_fn));
        DispatchedPipeline::new(dispatcher)
    }
}

impl<T> Default for DispatchedPipelineBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Pipeline;
    use crate::processors::NoOpProcessor;
    use crate::sinks::PrintSink;
    use crate::sources::RangeSource;

    #[tokio::test]
    async fn test_dispatcher_sink() {
        let dispatcher = Arc::new(DispatcherBuilder::new().broadcast());
        let mut sink = DispatcherSink::new(dispatcher, 5);

        // Test consuming items
        for i in 1..=3 {
            sink.write(i).await.unwrap();
        }

        // Test finish
        sink.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_dispatched_pipeline() {
        let dispatched = DispatchedPipelineBuilder::new().broadcast();

        // Subscribe a sink
        let print_sink = PrintSink::with_prefix("Test".to_string());
        let _sink_id = dispatched.subscribe_sink(print_sink, None).await.unwrap();

        // Create a dispatcher sink for use with a regular pipeline
        let dispatcher_sink = dispatched.create_sink(1);

        // Create a simple pipeline
        let source = RangeSource::new(1..6);
        let processor = NoOpProcessor::new();
        let pipeline = Pipeline::new(source, processor);

        // Run the pipeline through the dispatcher
        pipeline.sink(dispatcher_sink).await.unwrap();
    }
}
