//! Pipeline orchestration and execution.
//!
//! This module provides the core pipeline abstraction that connects sources,
//! processors, and sinks with configurable backpressure and concurrency control.

use crate::error::{Error, Result};
use crate::traits::{Processor, Sink, Source};
use std::time::Duration;
use tokio::sync::mpsc;

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

/// A pipeline connects sources, processors, and sinks with configurable
/// backpressure and concurrency control.
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

    /// Run the pipeline with a sink
    pub async fn sink<C>(self, consumer: C) -> Result<()>
    where
        C: Sink<Item = R::Output> + Send + Sync + 'static,
    {
        self.run_with_sink(consumer).await
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
        let fail_fast = config.fail_fast;

        loop {
            let produce_result =
                tokio::time::timeout(config.operation_timeout, source.produce()).await;

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
                        sink.consume(output).await?;
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
                        sink.consume(output).await?;
                    }
                    sink.finish().await?;
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

/// A pipeline that runs source and sink concurrently
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
        let ConcurrentPipeline { source, sink, .. } = self;

        let source_handle =
            tokio::spawn(async move { Self::run_source(source, tx, fail_fast).await });

        let sink_handle = tokio::spawn(async move { Self::run_sink(sink, rx, fail_fast).await });

        let (source_result, sink_result) = tokio::join!(source_handle, sink_handle);

        match (source_result, sink_result) {
            (Ok(Ok(())), Ok(Ok(()))) => Ok(()),
            (Ok(Err(e)), _) | (_, Ok(Err(e))) => Err(e),
            (Err(e), _) | (_, Err(e)) => Err(Error::custom(format!("Task panicked: {}", e))),
        }
    }

    async fn run_source(mut source: P, tx: mpsc::Sender<P::Item>, fail_fast: bool) -> Result<()> {
        loop {
            match source.produce().await {
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

    async fn run_sink(mut sink: C, mut rx: mpsc::Receiver<P::Item>, fail_fast: bool) -> Result<()> {
        while let Some(item) = rx.recv().await {
            if let Err(e) = sink.consume(item).await {
                if fail_fast {
                    return Err(e);
                }
            }
        }
        sink.finish().await
    }
}

/// A stage that combines a source and processor
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

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        match self.source.produce().await? {
            Some(item) => {
                let outputs = self.processor.process(item).await?;
                if outputs.is_empty() {
                    self.produce().await
                } else {
                    Ok(outputs.into_iter().next())
                }
            }
            None => {
                let outputs = self.processor.finish().await?;
                if outputs.is_empty() {
                    Ok(None)
                } else {
                    Ok(outputs.into_iter().next())
                }
            }
        }
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
