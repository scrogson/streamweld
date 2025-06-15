//! Core traits for the producer/consumer system.
//!
//! This module defines the fundamental abstractions that enable demand-driven
//! data processing pipelines with automatic backpressure control.

use crate::error::Result;
use async_trait::async_trait;

/// A producer generates items on demand.
///
/// Producers are pull-based - they only generate items when explicitly
/// requested by downstream consumers, enabling natural backpressure control.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use streamweld::error::Result;
/// use streamweld::traits::Producer;
///
/// struct CounterProducer {
///     current: u64,
///     max: u64,
/// }
///
/// #[async_trait]
/// impl Producer for CounterProducer {
///     type Item = u64;
///
///     async fn produce(&mut self) -> Result<Option<Self::Item>> {
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
pub trait Producer {
    /// The type of items this producer generates
    type Item: Send + 'static;

    /// Produce the next item, or None if the producer is exhausted.
    ///
    /// This method should be cheap to call repeatedly and should handle
    /// backpressure by only producing when called.
    async fn produce(&mut self) -> Result<Option<Self::Item>>;
}

/// A consumer processes items from upstream.
///
/// Consumers represent the demand side of the pipeline - they pull items
/// from producers and process them.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use streamweld::error::Result;
/// use streamweld::traits::Consumer;
///
/// struct LogConsumer;
///
/// #[async_trait]
/// impl Consumer for LogConsumer {
///     type Item = String;
///
///     async fn consume(&mut self, item: Self::Item) -> Result<()> {
///         println!("Consumed: {}", item);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Consumer {
    /// The type of items this consumer accepts
    type Item: Send + 'static;

    /// Process a single item.
    ///
    /// This method should handle the item completely - if it returns Ok(()),
    /// the item is considered successfully processed.
    async fn consume(&mut self, item: Self::Item) -> Result<()>;

    /// Called when the upstream producer is exhausted.
    ///
    /// This allows consumers to perform cleanup or flush any buffered state.
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A processor transforms items (consumer + producer combined).
///
/// This is equivalent to GenStage's producer_consumer - it consumes items
/// from upstream, transforms them, and produces new items for downstream.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use streamweld::error::Result;
/// use streamweld::traits::Processor;
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

/// Extension trait for composing producers with processors and consumers.
pub trait ProducerExt: Producer + Sized {
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

    /// Chain with another producer
    fn chain<P2>(self, other: P2) -> Chain<Self, P2>
    where
        P2: Producer<Item = Self::Item>;
}

/// Extension trait for consumers
pub trait ConsumerExt: Consumer + Sized {
    /// Create a consumer that applies a function to each item before consuming
    fn contramap<F, T>(self, f: F) -> Contramap<Self, F, T>
    where
        F: FnMut(T) -> Self::Item + Send,
        T: Send + 'static;
}

// Combinator implementations would go here
// For brevity, I'll define the types but implement them in the impls module

pub struct Map<P, F> {
    pub producer: P,
    pub f: F,
}

pub struct Filter<P, F> {
    pub producer: P,
    pub predicate: F,
}

pub struct Take<P> {
    pub producer: P,
    pub remaining: usize,
}

pub struct Chain<P1, P2> {
    pub first: Option<P1>,
    pub second: P2,
}

pub struct Contramap<C, F, T> {
    pub consumer: C,
    pub f: F,
    pub _phantom: std::marker::PhantomData<T>,
}

// Auto-implement ProducerExt for all Producers
impl<P: Producer> ProducerExt for P {
    fn map<F, U>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> U + Send,
        U: Send + 'static,
    {
        Map { producer: self, f }
    }

    fn filter<F>(self, predicate: F) -> Filter<Self, F>
    where
        F: FnMut(&Self::Item) -> bool + Send,
    {
        Filter {
            producer: self,
            predicate,
        }
    }

    fn take(self, n: usize) -> Take<Self> {
        Take {
            producer: self,
            remaining: n,
        }
    }

    fn chain<P2>(self, other: P2) -> Chain<Self, P2>
    where
        P2: Producer<Item = Self::Item>,
    {
        Chain {
            first: Some(self),
            second: other,
        }
    }
}

impl<C: Consumer> ConsumerExt for C {
    fn contramap<F, T>(self, f: F) -> Contramap<Self, F, T>
    where
        F: FnMut(T) -> Self::Item + Send,
        T: Send + 'static,
    {
        Contramap {
            consumer: self,
            f,
            _phantom: std::marker::PhantomData,
        }
    }
}
