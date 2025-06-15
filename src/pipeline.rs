//! Pipeline orchestration and execution.
//!
//! This module provides the core pipeline abstraction that connects producers,
//! processors, and consumers with configurable backpressure and concurrency control.

use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::error::{Error, Result};
use crate::traits::{Consumer, Processor, Producer};

/// Configuration for pipeline execution
#[derive(Clone)]
pub struct PipelineConfig {
    /// Maximum number of items to buffer between stages
    pub buffer_size: usize,
    /// Maximum time to wait for an operation
    pub operation_timeout: Duration,
    /// Maximum number of concurrent operations
    pub max_concurrency: usize,
    /// Whether to fail fast on errors
    pub fail_fast: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            operation_timeout: Duration::from_secs(30),
            max_concurrency: 1,
            fail_fast: true,
        }
    }
}

/// A pipeline connects producers, processors, and consumers with configurable
/// backpressure and concurrency control.
pub struct Pipeline<P, R> {
    stage: P,
    processor: R,
    config: PipelineConfig,
}

impl<P, R> Pipeline<P, R>
where
    P: Producer + Send + Sync + 'static,
    P::Item: Send + 'static,
    R: Processor<Input = P::Item> + Send + Sync + 'static,
    R::Output: Send + 'static,
{
    /// Create a new pipeline
    pub fn new(stage: P, processor: R) -> Self {
        Self {
            stage,
            processor,
            config: PipelineConfig::default(),
        }
    }

    /// Set the buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set the operation timeout
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.config.operation_timeout = timeout;
        self
    }

    /// Set the maximum concurrency
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.config.max_concurrency = max;
        self
    }

    /// Set whether to fail fast on errors
    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.config.fail_fast = fail_fast;
        self
    }

    /// Run the pipeline with a consumer
    pub async fn sink<C>(self, consumer: C) -> Result<()>
    where
        C: Consumer<Item = R::Output> + Send + Sync + 'static,
    {
        self.run_with_consumer(consumer).await
    }

    async fn run_with_consumer<C>(self, mut consumer: C) -> Result<()>
    where
        C: Consumer<Item = R::Output> + Send + Sync + 'static,
    {
        let Pipeline {
            stage: mut producer,
            processor: mut processor,
            config,
        } = self;
        let fail_fast = config.fail_fast;

        loop {
            let produce_result =
                tokio::time::timeout(config.operation_timeout, producer.produce()).await;

            let result = match produce_result {
                Ok(r) => r,
                Err(_) => {
                    return Err(Error::Timeout {
                        duration_ms: config.operation_timeout.as_millis() as u64,
                    })
                }
            };

            match result {
                Ok(Some(item)) => {
                    let process_result =
                        tokio::time::timeout(config.operation_timeout, processor.process(item))
                            .await;
                    let outputs = match process_result {
                        Ok(r) => r?,
                        Err(_) => {
                            return Err(Error::Timeout {
                                duration_ms: config.operation_timeout.as_millis() as u64,
                            })
                        }
                    };
                    for output in outputs {
                        consumer.consume(output).await?;
                    }
                }
                Ok(None) => {
                    let finish_result =
                        tokio::time::timeout(config.operation_timeout, processor.finish()).await;
                    let final_outputs = match finish_result {
                        Ok(r) => r?,
                        Err(_) => {
                            return Err(Error::Timeout {
                                duration_ms: config.operation_timeout.as_millis() as u64,
                            })
                        }
                    };
                    for output in final_outputs {
                        consumer.consume(output).await?;
                    }
                    consumer.finish().await?;
                    break;
                }
                Err(e) => {
                    if fail_fast {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }
}

/// A pipeline that runs producer and consumer concurrently
pub struct ConcurrentPipeline<P, C> {
    producer: P,
    consumer: C,
    config: PipelineConfig,
}

impl<P, C> ConcurrentPipeline<P, C>
where
    P: Producer + Send + Sync + 'static,
    P::Item: Send + 'static,
    C: Consumer<Item = P::Item> + Send + Sync + 'static,
{
    /// Create a new concurrent pipeline
    pub fn new(producer: P, consumer: C) -> Self {
        Self {
            producer,
            consumer,
            config: PipelineConfig::default(),
        }
    }

    /// Set the buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set the operation timeout
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.config.operation_timeout = timeout;
        self
    }

    /// Set the maximum concurrency
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.config.max_concurrency = max;
        self
    }

    /// Set whether to fail fast on errors
    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.config.fail_fast = fail_fast;
        self
    }

    /// Run the pipeline
    pub async fn run(self) -> Result<()> {
        let (tx, rx) = mpsc::channel(self.config.buffer_size);
        let fail_fast = self.config.fail_fast;
        let ConcurrentPipeline {
            producer, consumer, ..
        } = self;

        let producer_handle =
            tokio::spawn(async move { Self::run_producer(producer, tx, fail_fast).await });

        let consumer_handle =
            tokio::spawn(async move { Self::run_consumer(consumer, rx, fail_fast).await });

        let (producer_result, consumer_result) = tokio::join!(producer_handle, consumer_handle);

        match (producer_result, consumer_result) {
            (Ok(Ok(())), Ok(Ok(()))) => Ok(()),
            (Ok(Err(e)), _) | (_, Ok(Err(e))) => Err(e),
            (Err(e), _) | (_, Err(e)) => Err(Error::custom(format!("Task panicked: {}", e))),
        }
    }

    async fn run_producer(
        mut producer: P,
        tx: mpsc::Sender<P::Item>,
        fail_fast: bool,
    ) -> Result<()> {
        loop {
            match producer.produce().await {
                Ok(Some(item)) => {
                    if tx.send(item).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    if fail_fast {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_consumer(
        mut consumer: C,
        mut rx: mpsc::Receiver<P::Item>,
        fail_fast: bool,
    ) -> Result<()> {
        while let Some(item) = rx.recv().await {
            if let Err(e) = consumer.consume(item).await {
                if fail_fast {
                    return Err(e);
                }
            }
        }
        consumer.finish().await
    }
}

/// A stage that combines a producer and processor
pub struct PipelineStage<P, R> {
    producer: P,
    processor: R,
}

#[async_trait]
impl<P, R> Producer for PipelineStage<P, R>
where
    P: Producer + Send + Sync + 'static,
    R: Processor<Input = P::Item> + Send + Sync + 'static,
{
    type Item = R::Output;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        // Keep trying to produce until we get an output or the producer is exhausted
        loop {
            match self.producer.produce().await? {
                Some(input) => {
                    let outputs = self.processor.process(input).await?;
                    if !outputs.is_empty() {
                        // For simplicity, return the first output
                        // In a real implementation, you might want to buffer multiple outputs
                        return Ok(Some(outputs.into_iter().next().unwrap()));
                    }
                    // If no outputs, continue to next input
                }
                None => {
                    // Producer exhausted, check if processor has final outputs
                    let final_outputs = self.processor.finish().await?;
                    if !final_outputs.is_empty() {
                        return Ok(Some(final_outputs.into_iter().next().unwrap()));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

/// Extension trait for creating pipelines
pub trait PipelineExt<T> {
    /// Create a pipeline from any producer
    fn into_pipeline(self) -> Pipeline<Self, T>
    where
        Self: Sized;
}

impl<P> PipelineExt<P> for P
where
    P: Producer + Processor<Input = P::Item, Output = P::Item> + Send + Sync + Clone + 'static,
    P::Item: Send + 'static,
{
    fn into_pipeline(self) -> Pipeline<Self, P> {
        Pipeline::new(self.clone(), self)
    }
}
