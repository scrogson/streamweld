//! Combinator implementations for sources and processors.
//!
//! This module provides composable building blocks for transforming data streams
//! using demand-driven processing with efficient batch handling.

use std::marker::PhantomData;

use crate::core::{Processor, Result, Source};
use async_trait::async_trait;

/// A source that maps items through a function
pub struct MapSource<P, F> {
    source: P,
    f: F,
}

impl<P, F> MapSource<P, F> {
    /// Create a new map source
    pub fn new(source: P, f: F) -> Self {
        Self { source, f }
    }
}

#[async_trait]
impl<P, F, T, U> Source for MapSource<P, F>
where
    P: Source<Item = T> + Send,
    F: FnMut(T) -> U + Send,
    T: Send + 'static,
    U: Send + 'static,
{
    type Item = U;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let items = self.source.handle_demand(demand).await?;
        Ok(items.into_iter().map(&mut self.f).collect())
    }
}

/// A source that filters items based on a predicate
pub struct FilterSource<P, F> {
    source: P,
    predicate: F,
}

impl<P, F> FilterSource<P, F> {
    /// Create a new filter source
    pub fn new(source: P, predicate: F) -> Self {
        Self { source, predicate }
    }
}

#[async_trait]
impl<P, F, T> Source for FilterSource<P, F>
where
    P: Source<Item = T> + Send,
    F: FnMut(&T) -> bool + Send,
    T: Send + 'static,
{
    type Item = T;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut results = Vec::new();
        let mut requested = demand;

        // Keep requesting more items until we have enough or source is exhausted
        while results.len() < demand && requested > 0 {
            let items = self.source.handle_demand(requested).await?;

            if items.is_empty() {
                break; // Source exhausted
            }

            let filtered: Vec<T> = items.into_iter().filter(&mut self.predicate).collect();
            results.extend(filtered);

            // Adjust demand based on what we still need
            // Request more items if filtering reduced our results
            requested = demand.saturating_sub(results.len()).max(requested / 2);
        }

        // Return up to the requested demand
        if results.len() > demand {
            results.truncate(demand);
        }

        Ok(results)
    }
}

/// A source that takes only the first N items
pub struct TakeSource<P> {
    source: P,
    remaining: usize,
}

impl<P> TakeSource<P> {
    /// Create a new take source
    pub fn new(source: P, count: usize) -> Self {
        Self {
            source,
            remaining: count,
        }
    }
}

#[async_trait]
impl<P: Source + Send> Source for TakeSource<P> {
    type Item = P::Item;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        if self.remaining == 0 {
            return Ok(vec![]);
        }

        let to_request = demand.min(self.remaining);
        let items = self.source.handle_demand(to_request).await?;

        let actual_taken = items.len().min(self.remaining);
        self.remaining = self.remaining.saturating_sub(actual_taken);

        Ok(items.into_iter().take(actual_taken).collect())
    }
}

/// Extension trait for adding combinators to sources
pub trait SourceExt: Source {
    /// Map items through a function
    fn map<F, U>(self, f: F) -> MapSource<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> U + Send,
        U: Send + 'static,
    {
        MapSource::new(self, f)
    }

    /// Filter items based on a predicate
    fn filter<F>(self, predicate: F) -> FilterSource<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> bool + Send,
    {
        FilterSource::new(self, predicate)
    }

    /// Take only the first N items
    fn take(self, count: usize) -> TakeSource<Self>
    where
        Self: Sized,
    {
        TakeSource::new(self, count)
    }
}

impl<P: Source> SourceExt for P {}

/// A processor that transforms items through a mapping function
pub struct MapThroughProcessor<F, T, U> {
    f: F,
    _phantom: PhantomData<(T, U)>,
}

impl<F, T, U> MapThroughProcessor<F, T, U> {
    /// Create a new map-through processor
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T, U> Processor for MapThroughProcessor<F, T, U>
where
    F: FnMut(T) -> U + Send,
    T: Send + 'static,
    U: Send + 'static,
{
    type Input = T;
    type Output = U;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        Ok(items.into_iter().map(&mut self.f).collect())
    }
}

/// A processor that filters items through a predicate
pub struct FilterThroughProcessor<F, T> {
    predicate: F,
    _phantom: PhantomData<T>,
}

impl<F, T> FilterThroughProcessor<F, T> {
    /// Create a new filter-through processor
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> Processor for FilterThroughProcessor<F, T>
where
    F: FnMut(&T) -> bool + Send,
    T: Send + 'static,
{
    type Input = T;
    type Output = T;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        Ok(items.into_iter().filter(&mut self.predicate).collect())
    }
}

/// Extension trait for adding combinators to processors
pub trait ProcessorExt: Processor {
    /// Map outputs through a function
    fn map_output<F, U>(self, f: F) -> ChainProcessor<Self, MapThroughProcessor<F, Self::Output, U>>
    where
        Self: Sized,
        F: FnMut(Self::Output) -> U + Send,
        U: Send + 'static,
    {
        ChainProcessor::new(self, MapThroughProcessor::new(f))
    }

    /// Filter outputs based on a predicate
    fn filter_output<F>(
        self,
        predicate: F,
    ) -> ChainProcessor<Self, FilterThroughProcessor<F, Self::Output>>
    where
        Self: Sized,
        F: FnMut(&Self::Output) -> bool + Send,
    {
        ChainProcessor::new(self, FilterThroughProcessor::new(predicate))
    }
}

impl<P: Processor> ProcessorExt for P {}

/// A processor that chains two processors together
pub struct ChainProcessor<P1, P2> {
    first: P1,
    second: P2,
}

impl<P1, P2> ChainProcessor<P1, P2> {
    /// Create a new chain processor
    pub fn new(first: P1, second: P2) -> Self {
        Self { first, second }
    }
}

#[async_trait]
impl<P1, P2> Processor for ChainProcessor<P1, P2>
where
    P1: Processor + Send,
    P2: Processor<Input = P1::Output> + Send,
    P1::Input: Send + 'static,
    P1::Output: Send + 'static,
    P2::Output: Send + 'static,
{
    type Input = P1::Input;
    type Output = P2::Output;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let intermediate_results = self.first.process_batch(items).await?;
        if intermediate_results.is_empty() {
            return Ok(vec![]);
        }
        self.second.process_batch(intermediate_results).await
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        let first_final = self.first.finish().await?;
        if first_final.is_empty() {
            return self.second.finish().await;
        }

        let intermediate_results = self.second.process_batch(first_final).await?;
        let final_results = self.second.finish().await?;

        let mut combined = intermediate_results;
        combined.extend(final_results);
        Ok(combined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::RangeSource;

    #[tokio::test]
    async fn test_map_combinator() {
        let source = RangeSource::new(1..4);
        let mut mapped = source.map(|x| x * 2);

        let items = mapped.handle_demand(10).await.unwrap();
        assert_eq!(items, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn test_filter_combinator() {
        let source = RangeSource::new(1..7);
        let mut filtered = source.filter(|x| x % 2 == 0);

        let items = filtered.handle_demand(10).await.unwrap();
        assert_eq!(items, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn test_take_combinator() {
        let source = RangeSource::new(1..10);
        let mut taken = source.take(3);

        let items = taken.handle_demand(10).await.unwrap();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_complex_combinator_chain() {
        let source = RangeSource::new(1..21);
        let mut complex = source
            .filter(|x| x % 2 == 0) // Even numbers
            .map(|x| x * 3) // Multiply by 3
            .take(5); // Take first 5

        let items = complex.handle_demand(10).await.unwrap();
        assert_eq!(items, vec![6, 12, 18, 24, 30]); // 2*3, 4*3, 6*3, 8*3, 10*3
    }
}
