//! Integration between pipelines and dispatchers for multi-consumer patterns.

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::dispatcher::{ConsumerId, Dispatcher, DispatcherBuilder};
use crate::error::{Error, Result};
use crate::pipeline::{Pipeline, PipelineConfig};
use crate::traits::{Consumer, Producer};

/// A consumer that forwards items to a dispatcher
pub struct DispatcherConsumer<T> {
    dispatcher: Arc<Dispatcher<T>>,
    batch_size: usize,
    batch: Vec<T>,
}

impl<T> DispatcherConsumer<T>
where
    T: Send + 'static,
{
    /// Create a new dispatcher consumer
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

impl<T: Send + 'static> Consumer for DispatcherConsumer<T> {
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
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

/// Pipeline extension for dispatcher integration
pub trait PipelineDispatcherExt<T> {
    /// Send pipeline output to a dispatcher
    fn dispatch_to(self, dispatcher: Arc<Dispatcher<T>>) -> DispatchedPipeline<T>;

    /// Send pipeline output to a dispatcher with batching
    fn dispatch_to_batched(
        self,
        dispatcher: Arc<Dispatcher<T>>,
        batch_size: usize,
    ) -> DispatchedPipeline<T>;
}

impl<P> PipelineDispatcherExt<P::Item> for Pipeline<P>
where
    P: Producer,
    P::Item: Send + 'static,
{
    fn dispatch_to(self, dispatcher: Arc<Dispatcher<P::Item>>) -> DispatchedPipeline<P::Item> {
        DispatchedPipeline {
            pipeline: self,
            dispatcher,
            batch_size: 1,
        }
    }

    fn dispatch_to_batched(
        self,
        dispatcher: Arc<Dispatcher<P::Item>>,
        batch_size: usize,
    ) -> DispatchedPipeline<P::Item> {
        DispatchedPipeline {
            pipeline: self,
            dispatcher,
            batch_size,
        }
    }
}

/// A pipeline that outputs to a dispatcher
pub struct DispatchedPipeline<T> {
    pipeline: Pipeline<impl Producer<Item = T>>,
    dispatcher: Arc<Dispatcher<T>>,
    batch_size: usize,
}

impl<T> DispatchedPipeline<T>
where
    T: Send + 'static,
{
    /// Subscribe a consumer to the dispatcher
    pub async fn subscribe_consumer<C>(
        &self,
        consumer: C,
        partition: Option<String>,
    ) -> Result<ConsumerId>
    where
        C: Consumer<Item = T> + Send + 'static,
    {
        self.dispatcher
            .subscribe_consumer(consumer, partition)
            .await
    }

    /// Unsubscribe a consumer from the dispatcher
    pub async fn unsubscribe_consumer(&self, consumer_id: ConsumerId) -> Result<()> {
        self.dispatcher.unsubscribe_consumer(consumer_id).await
    }

    /// Run the pipeline and dispatch to consumers
    pub async fn run(self) -> Result<()> {
        let dispatcher_consumer = DispatcherConsumer::new(self.dispatcher, self.batch_size);
        self.pipeline.sink(dispatcher_consumer).await
    }

    /// Get the dispatcher for direct access
    pub fn dispatcher(&self) -> &Arc<Dispatcher<T>> {
        &self.dispatcher
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

    /// Set max demand per consumer
    pub fn max_demand(mut self, demand: usize) -> Self {
        self.dispatcher_builder = self.dispatcher_builder.max_demand(demand);
        self
    }

    /// Build a pipeline with demand-based dispatcher
    pub fn with_demand_dispatch<P>(self, pipeline: Pipeline<P>) -> DispatchedPipeline<T>
    where
        P: Producer<Item = T>,
        T: Send + 'static,
    {
        let dispatcher = Arc::new(self.dispatcher_builder.demand());
        pipeline.dispatch_to(dispatcher)
    }

    /// Build a pipeline with broadcast dispatcher
    pub fn with_broadcast_dispatch<P>(self, pipeline: Pipeline<P>) -> DispatchedPipeline<T>
    where
        P: Producer<Item = T>,
        T: Send + 'static,
    {
        let dispatcher = Arc::new(self.dispatcher_builder.broadcast());
        pipeline.dispatch_to(dispatcher)
    }

    /// Build a pipeline with partition dispatcher
    pub fn with_partition_dispatch<P, F>(
        self,
        pipeline: Pipeline<P>,
        partitions: Vec<String>,
        hash_fn: F,
    ) -> DispatchedPipeline<T>
    where
        P: Producer<Item = T>,
        T: Send + 'static,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        let dispatcher = Arc::new(self.dispatcher_builder.partition(partitions, hash_fn));
        pipeline.dispatch_to(dispatcher)
    }
}

impl<T> Default for DispatchedPipelineBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience functions for creating common dispatch patterns
impl<T: Send + 'static> DispatchedPipelineBuilder<T> {
    /// Create a load-balanced pipeline (demand dispatcher)
    pub fn load_balanced<P>(pipeline: Pipeline<P>) -> DispatchedPipeline<T>
    where
        P: Producer<Item = T>,
    {
        Self::new().with_demand_dispatch(pipeline)
    }

    /// Create a fan-out pipeline (broadcast dispatcher)
    pub fn fan_out<P>(pipeline: Pipeline<P>) -> DispatchedPipeline<T>
    where
        P: Producer<Item = T>,
    {
        Self::new().with_broadcast_dispatch(pipeline)
    }

    /// Create a sharded pipeline (partition dispatcher)
    pub fn sharded<P, F>(
        pipeline: Pipeline<P>,
        partitions: Vec<String>,
        hash_fn: F,
    ) -> DispatchedPipeline<T>
    where
        P: Producer<Item = T>,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        Self::new().with_partition_dispatch(pipeline, partitions, hash_fn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impls::consumers::PrintConsumer;
    use crate::impls::processors::MapProcessor;
    use crate::impls::producers::RangeProducer;

    #[tokio::test]
    async fn test_dispatched_pipeline() {
        let producer = RangeProducer::new(1..11);
        let processor = MapProcessor::new(|x| format!("Item-{}", x));

        let pipeline = Pipeline::new(producer).pipe(processor);
        let dispatched = DispatchedPipelineBuilder::load_balanced(pipeline);

        // Subscribe multiple consumers
        let consumer1 = PrintConsumer::with_prefix("Consumer-1");
        let consumer2 = PrintConsumer::with_prefix("Consumer-2");

        let _id1 = dispatched
            .subscribe_consumer(consumer1, None)
            .await
            .unwrap();
        let _id2 = dispatched
            .subscribe_consumer(consumer2, None)
            .await
            .unwrap();

        // Run the pipeline - items should be distributed between consumers
        dispatched.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_fan_out_pipeline() {
        let producer = RangeProducer::new(1..6);
        let pipeline = Pipeline::new(producer);
        let dispatched = DispatchedPipelineBuilder::fan_out(pipeline);

        // Subscribe multiple consumers - all should receive all items
        let logger = PrintConsumer::with_prefix("Logger");
        let metrics = PrintConsumer::with_prefix("Metrics");

        dispatched.subscribe_consumer(logger, None).await.unwrap();
        dispatched.subscribe_consumer(metrics, None).await.unwrap();

        dispatched.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_sharded_pipeline() {
        #[derive(Clone)]
        struct Item {
            key: String,
            value: i64,
        }

        let items = vec![
            Item {
                key: "A".to_string(),
                value: 1,
            },
            Item {
                key: "B".to_string(),
                value: 2,
            },
            Item {
                key: "A".to_string(),
                value: 3,
            },
            Item {
                key: "C".to_string(),
                value: 4,
            },
        ];

        let producer = crate::impls::producers::VecProducer::new(items);
        let pipeline = Pipeline::new(producer);

        let partitions = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let dispatched = DispatchedPipelineBuilder::sharded(pipeline, partitions, |item: &Item| {
            item.key.clone()
        });

        // Subscribe consumers to specific partitions
        let consumer_a = PrintConsumer::with_prefix("Partition-A");
        let consumer_b = PrintConsumer::with_prefix("Partition-B");
        let consumer_c = PrintConsumer::with_prefix("Partition-C");

        dispatched
            .subscribe_consumer(consumer_a, Some("A".to_string()))
            .await
            .unwrap();
        dispatched
            .subscribe_consumer(consumer_b, Some("B".to_string()))
            .await
            .unwrap();
        dispatched
            .subscribe_consumer(consumer_c, Some("C".to_string()))
            .await
            .unwrap();

        dispatched.run().await.unwrap();
    }
}
