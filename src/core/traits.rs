//! Core traits for the source/sink system.
//!
//! This module defines the fundamental abstractions that enable demand-driven
//! data processing pipelines with automatic backpressure control.
//!
//! This system is inspired by Elixir's GenStage, using explicit demand signaling
//! for efficient batch processing with natural backpressure.

use crate::core::error::Result;
use async_trait::async_trait;

/// A source generates items on demand using GenStage-style demand signaling.
///
/// Sources respond to explicit demand requests from downstream sinks,
/// enabling efficient batch processing and natural backpressure control.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use streamweld::core::{Result, Source};
///
/// struct CounterSource {
///     current: u64,
///     max: u64,
/// }
///
/// #[async_trait]
/// impl Source for CounterSource {
///     type Item = u64;
///
///     async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
///         let mut items = Vec::with_capacity(demand);
///         
///         for _ in 0..demand {
///             if self.current <= self.max {
///                 items.push(self.current);
///                 self.current += 1;
///             } else {
///                 break; // Source exhausted
///             }
///         }
///         
///         Ok(items)
///     }
/// }
/// ```
#[async_trait]
pub trait Source {
    /// The type of items this source generates
    type Item: Send + 'static;

    /// Handle demand for multiple items (GenStage-style).
    ///
    /// This is the primary method that responds to explicit demand signals.
    /// Sources should return up to `demand` items, or fewer if exhausted.
    /// An empty Vec indicates the source is completely exhausted.
    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>>;

    /// Get the next single item (convenience method).
    ///
    /// This is a convenience wrapper around handle_demand for single-item use cases.
    /// Most efficient usage should prefer handle_demand with larger batch sizes.
    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let items = self.handle_demand(1).await?;
        Ok(items.into_iter().next())
    }
}

/// A sink processes items from upstream with batch-first design.
///
/// Sinks represent the demand side of the pipeline - they process items
/// in batches for efficiency, with single-item writes as a convenience.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use streamweld::core::{Result, Sink};
///
/// struct LogSink;
///
/// #[async_trait]
/// impl Sink for LogSink {
///     type Item = String;
///
///     async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
///         for item in items {
///             println!("Logged: {}", item);
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Sink {
    /// The type of items this sink accepts
    type Item: Send + 'static;

    /// Write a batch of items (primary method).
    ///
    /// This is the primary method for efficient batch processing.
    /// Sinks should process all items in the batch together when possible.
    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()>;

    /// Write a single item (convenience method).
    ///
    /// This is a convenience wrapper around write_batch for single-item use cases.
    /// Most efficient usage should prefer write_batch with larger batches.
    async fn write(&mut self, item: Self::Item) -> Result<()> {
        self.write_batch(vec![item]).await
    }

    /// Called when the upstream source is exhausted.
    ///
    /// This allows sinks to perform cleanup or flush any buffered state.
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A processor transforms items (sink + source combined) with batch processing.
///
/// This is equivalent to GenStage's source_sink - it handles demand
/// from downstream, processes items in batches, and produces new items.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use streamweld::core::{Result, Processor};
///
/// struct DoubleProcessor;
///
/// #[async_trait]
/// impl Processor for DoubleProcessor {
///     type Input = i32;
///     type Output = i32;
///
///     async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
///         Ok(items.into_iter().map(|x| x * 2).collect())
///     }
/// }
/// ```
#[async_trait]
pub trait Processor {
    /// The type of items this processor accepts
    type Input: Send + 'static;
    /// The type of items this processor produces
    type Output: Send + 'static;

    /// Process a batch of input items and produce output items (primary method).
    ///
    /// This is the primary method for efficient batch processing.
    /// Returns a Vec of all output items from processing the input batch.
    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>>;

    /// Process a single input item (convenience method).
    ///
    /// This is a convenience wrapper for single-item processing.
    /// Default implementation calls process_batch with a single item.
    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        let outputs = self.process_batch(vec![item]).await?;
        Ok(outputs)
    }

    /// Called when upstream is exhausted, allowing final output generation.
    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        Ok(vec![])
    }
}

/// Extension trait for sources that provides combinator methods
pub trait SourceExt: Source {
    /// Map items through a function
    fn map<F, U>(self, f: F) -> crate::processors::combinators::MapSource<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> U + Send,
        U: Send + 'static,
    {
        crate::processors::combinators::MapSource::new(self, f)
    }

    /// Filter items based on a predicate
    fn filter<F>(self, predicate: F) -> crate::processors::combinators::FilterSource<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> bool + Send,
    {
        crate::processors::combinators::FilterSource::new(self, predicate)
    }

    /// Take only the first N items
    fn take(self, count: usize) -> crate::processors::combinators::TakeSource<Self>
    where
        Self: Sized,
    {
        crate::processors::combinators::TakeSource::new(self, count)
    }
}

impl<P: Source> SourceExt for P {}

/// A source that produces events from a function.
///
/// This is a convenience trait for creating sources from functions.
pub trait SourceFromFn: Send + Sync {
    /// The type of events produced by this source
    type Item: Send + Sync + Clone;

    /// Create a new source from a function
    fn source_from_fn<F>(f: F) -> Self
    where
        F: Fn() -> Result<Vec<Self::Item>> + Send + Sync + 'static;
}

/// A sink that consumes events using a function.
///
/// This is a convenience trait for creating sinks from functions.
pub trait SinkFromFn: Send + Sync {
    /// The type of events consumed by this sink
    type Item: Send + Sync + Clone;

    /// Create a new sink from a function
    fn sink_from_fn<F>(f: F) -> Self
    where
        F: Fn(Vec<Self::Item>) -> Result<()> + Send + Sync + 'static;
}
