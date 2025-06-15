//! Core traits for the source/sink system.
//!
//! This module defines the fundamental abstractions that enable demand-driven
//! data processing pipelines with automatic backpressure control.

use crate::core::error::Result;
use async_trait::async_trait;

/// A source generates items on demand.
///
/// Sources are pull-based - they only generate items when explicitly
/// requested by downstream sinks, enabling natural backpressure control.
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
///     async fn next(&mut self) -> Result<Option<Self::Item>> {
///         if self.current <= self.max {
///             let item = self.current;
///             self.current += 1;
///             Ok(Some(item))
///         } else {
///             Ok(None) // Signal completion
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Source {
    /// The type of items this source generates
    type Item: Send + 'static;

    /// Get the next item, or None if the source is exhausted.
    ///
    /// This method should be cheap to call repeatedly and should handle
    /// backpressure by only generating when called.
    async fn next(&mut self) -> Result<Option<Self::Item>>;
}

/// A sink processes items from upstream.
///
/// Sinks represent the demand side of the pipeline - they pull items
/// from sources and process them.
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
///     async fn write(&mut self, item: Self::Item) -> Result<()> {
///         println!("Logged: {}", item);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Sink {
    /// The type of items this sink accepts
    type Item: Send + 'static;

    /// Write a single item.
    ///
    /// This method should handle the item completely - if it returns Ok(()),
    /// the item is considered successfully processed.
    async fn write(&mut self, item: Self::Item) -> Result<()>;

    /// Called when the upstream source is exhausted.
    ///
    /// This allows sinks to perform cleanup or flush any buffered state.
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A processor transforms items (sink + source combined).
///
/// This is equivalent to GenStage's source_sink - it consumes items
/// from upstream, transforms them, and produces new items for downstream.
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
///     async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
///         Ok(vec![item * 2])  // Transform input to output
///     }
/// }
/// ```
#[async_trait]
pub trait Processor {
    /// The type of items this processor accepts
    type Input: Send + 'static;
    /// The type of items this processor produces
    type Output: Send + 'static;

    /// Process an input item and produce zero or more output items.
    ///
    /// Returning an empty Vec means the item was consumed but produced no output.
    /// This enables filtering and batching behaviors.
    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>>;

    /// Called when upstream is exhausted, allowing final output generation.
    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        Ok(vec![])
    }
}

/// Extension trait for composing sources with processors and sinks.
pub trait SourceExt: Source + Sized {
    /// Map items through a function
    fn map<F, U>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> U + Send,
        U: Send + 'static;

    /// Filter items with a predicate
    fn filter<F>(self, predicate: F) -> Filter<Self, F>
    where
        F: FnMut(&Self::Item) -> bool + Send;

    /// Take only the first n items
    fn take(self, n: usize) -> Take<Self>;

    /// Chain with another source
    fn chain<P2>(self, other: P2) -> Chain<Self, P2>
    where
        P2: Source<Item = Self::Item>;
}

/// Extension trait for sinks
pub trait SinkExt: Sink + Sized {
    /// Create a sink that applies a function to each item before consuming
    fn contramap<F, T>(self, f: F) -> Contramap<Self, F, T>
    where
        F: FnMut(T) -> Self::Item + Send,
        T: Send + 'static;
}

// Combinator implementations would go here
// For brevity, I'll define the types but implement them in the impls module

pub struct Map<P, F> {
    pub source: P,
    pub f: F,
}

pub struct Filter<P, F> {
    pub source: P,
    pub predicate: F,
}

pub struct Take<P> {
    pub source: P,
    pub remaining: usize,
}

pub struct Chain<P1, P2> {
    pub first: Option<P1>,
    pub second: P2,
}

pub struct Contramap<C, F, T> {
    pub sink: C,
    pub f: F,
    pub _phantom: std::marker::PhantomData<T>,
}

// Auto-implement SourceExt for all Sources
impl<P: Source> SourceExt for P {
    fn map<F, U>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> U + Send,
        U: Send + 'static,
    {
        Map { source: self, f }
    }

    fn filter<F>(self, predicate: F) -> Filter<Self, F>
    where
        F: FnMut(&Self::Item) -> bool + Send,
    {
        Filter {
            source: self,
            predicate,
        }
    }

    fn take(self, n: usize) -> Take<Self> {
        Take {
            source: self,
            remaining: n,
        }
    }

    fn chain<P2>(self, other: P2) -> Chain<Self, P2>
    where
        P2: Source<Item = Self::Item>,
    {
        Chain {
            first: Some(self),
            second: other,
        }
    }
}

impl<C: Sink> SinkExt for C {
    fn contramap<F, T>(self, f: F) -> Contramap<Self, F, T>
    where
        F: FnMut(T) -> Self::Item + Send,
        T: Send + 'static,
    {
        Contramap {
            sink: self,
            f,
            _phantom: std::marker::PhantomData,
        }
    }
}

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
