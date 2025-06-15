//! Pipeline orchestration and execution.
//!
//! This module provides the core pipeline abstraction that connects sources,
//! processors, and sinks with configurable backpressure and demand-driven
//! processing using GenStage-style batch handling.

pub mod dispatcher;

use std::time::Duration;
use tokio::sync::mpsc;

use crate::core::{Error, Processor, Result, Sink, Source};

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
    /// Demand batch size for efficient processing
    pub demand_batch_size: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            operation_timeout: Duration::from_secs(30),
            max_concurrency: 1,
            fail_fast: true,
            demand_batch_size: 100, // GenStage-style batch processing
        }
    }
}

/// A pipeline connects sources, processors, and sinks with configurable
/// backpressure and demand-driven batch processing.
pub struct Pipeline<P, R> {
    stage: P,
    processor: R,
    config: PipelineConfig,
}

impl<P, R> Pipeline<P, R>
where
    P: Source + Send + Sync + 'static,
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

    /// Set the demand batch size for efficient processing
    pub fn demand_batch_size(mut self, size: usize) -> Self {
        self.config.demand_batch_size = size;
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

    /// Run the pipeline with a sink using demand-driven processing
    pub async fn sink<C>(self, sink: C) -> Result<()>
    where
        C: Sink<Item = R::Output> + Send + Sync + 'static,
    {
        self.run_with_sink(sink).await
    }

    async fn run_with_sink<C>(self, mut sink: C) -> Result<()>
    where
        C: Sink<Item = R::Output> + Send + Sync + 'static,
    {
        let Pipeline {
            stage: mut source,
            mut processor,
            config,
        } = self;
        let _fail_fast = config.fail_fast;
        let demand_size = config.demand_batch_size;

        loop {
            // Use GenStage-style demand signaling for batch processing
            let demand_result =
                tokio::time::timeout(config.operation_timeout, source.handle_demand(demand_size))
                    .await;

            let items = match demand_result {
                Ok(r) => r?,
                Err(_) => {
                    return Err(Error::Timeout {
                        duration_ms: config.operation_timeout.as_millis() as u64,
                    })
                }
            };

            if items.is_empty() {
                // Source exhausted, process any final outputs
                let final_outputs = processor.finish().await?;
                if !final_outputs.is_empty() {
                    sink.write_batch(final_outputs).await?;
                }
                sink.finish().await?;
                break;
            }

            // Process the batch of items
            let process_result =
                tokio::time::timeout(config.operation_timeout, processor.process_batch(items))
                    .await;

            let outputs = match process_result {
                Ok(r) => r?,
                Err(_) => {
                    return Err(Error::Timeout {
                        duration_ms: config.operation_timeout.as_millis() as u64,
                    })
                }
            };

            // Write outputs to sink in batch
            if !outputs.is_empty() {
                sink.write_batch(outputs).await?;
            }
        }

        Ok(())
    }
}

/// A pipeline that runs source and sink concurrently with demand-driven processing
pub struct ConcurrentPipeline<P, C> {
    source: P,
    sink: C,
    config: PipelineConfig,
}

impl<P, C> ConcurrentPipeline<P, C>
where
    P: Source + Send + Sync + 'static,
    P::Item: Send + 'static,
    C: Sink<Item = P::Item> + Send + Sync + 'static,
{
    /// Create a new concurrent pipeline
    pub fn new(source: P, sink: C) -> Self {
        Self {
            source,
            sink,
            config: PipelineConfig::default(),
        }
    }

    /// Set the buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set the demand batch size
    pub fn demand_batch_size(mut self, size: usize) -> Self {
        self.config.demand_batch_size = size;
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

    /// Run the pipeline with demand-driven concurrent processing
    pub async fn run(self) -> Result<()> {
        let (tx, rx) = mpsc::channel(self.config.buffer_size);
        let fail_fast = self.config.fail_fast;
        let demand_size = self.config.demand_batch_size;
        let ConcurrentPipeline { source, sink, .. } = self;

        let source_handle =
            tokio::spawn(async move { Self::run_source(source, tx, fail_fast, demand_size).await });

        let sink_handle = tokio::spawn(async move { Self::run_sink(sink, rx, fail_fast).await });

        let (source_result, sink_result) = tokio::join!(source_handle, sink_handle);

        match (source_result, sink_result) {
            (Ok(Ok(())), Ok(Ok(()))) => Ok(()),
            (Ok(Err(e)), _) | (_, Ok(Err(e))) => Err(e),
            (Err(e), _) | (_, Err(e)) => Err(Error::custom(format!("Task panicked: {}", e))),
        }
    }

    async fn run_source(
        mut source: P,
        tx: mpsc::Sender<P::Item>,
        fail_fast: bool,
        demand_size: usize,
    ) -> Result<()> {
        loop {
            match source.handle_demand(demand_size).await {
                Ok(items) => {
                    if items.is_empty() {
                        break; // Source exhausted
                    }

                    // Send items individually to maintain backpressure
                    for item in items {
                        if tx.send(item).await.is_err() {
                            return Ok(()); // Receiver closed
                        }
                    }
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

    async fn run_sink(mut sink: C, mut rx: mpsc::Receiver<P::Item>, fail_fast: bool) -> Result<()> {
        let mut batch = Vec::new();
        let batch_size = 50; // Configurable batch size for sink processing

        while let Some(item) = rx.recv().await {
            batch.push(item);

            // Process batch when full or channel is empty (for low latency)
            if batch.len() >= batch_size || rx.is_empty() {
                if let Err(e) = sink.write_batch(std::mem::take(&mut batch)).await {
                    if fail_fast {
                        return Err(e);
                    }
                }
            }
        }

        // Process any remaining items
        if !batch.is_empty() {
            sink.write_batch(batch).await?;
        }

        sink.finish().await
    }
}

/// A stage that combines a source and processor with demand-driven processing
pub struct PipelineStage<P, R> {
    source: P,
    processor: R,
}

#[async_trait::async_trait]
impl<P, R> Source for PipelineStage<P, R>
where
    P: Source + Send + Sync + 'static,
    P::Item: Send + 'static,
    R: Processor<Input = P::Item> + Send + Sync + 'static,
    R::Output: Send + 'static,
{
    type Item = R::Output;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut outputs = Vec::new();
        let mut requested = demand;

        while outputs.len() < demand && requested > 0 {
            // Request items from source
            let items = self.source.handle_demand(requested).await?;

            if items.is_empty() {
                // Source exhausted, get final outputs
                let final_outputs = self.processor.finish().await?;
                outputs.extend(final_outputs);
                break;
            }

            // Process the batch
            let processed = self.processor.process_batch(items).await?;
            outputs.extend(processed);

            // Adjust demand based on what we still need
            requested = demand.saturating_sub(outputs.len());
        }

        // Return up to the requested demand
        if outputs.len() > demand {
            outputs.truncate(demand);
        }

        Ok(outputs)
    }
}

/// Extension trait for creating pipelines
pub trait PipelineExt<T> {
    /// Create a pipeline from any source
    fn into_pipeline(self) -> Pipeline<Self, T>
    where
        Self: Sized + Default;
}

// Commenting out the blanket impl as it may not be correct for all use cases
// impl<P> PipelineExt<P> for P
// where
//     P: Source + Send + Sync + 'static + Default,
//     P::Item: Send + 'static,
// {
//     fn into_pipeline(self) -> Pipeline<Self, P> {
//         Pipeline::new(self, P::default())
//     }
// }
