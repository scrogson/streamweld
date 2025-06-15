//! Source implementations for the streamweld library.
//!
//! This module provides concrete implementations of sources that generate data
//! for processing pipelines using GenStage-style demand signaling.

use async_trait::async_trait;
use std::collections::VecDeque;
use std::ops::Range;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::core::{Result, Source};

/// A source that generates numbers from a range
pub struct RangeSource {
    range: Range<i64>,
}

impl RangeSource {
    /// Create a new range source
    pub fn new(range: Range<i64>) -> Self {
        Self { range }
    }
}

#[async_trait]
impl Source for RangeSource {
    type Item = i64;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut items = Vec::with_capacity(demand);

        for _ in 0..demand {
            if let Some(item) = self.range.next() {
                items.push(item);
            } else {
                break; // Range exhausted
            }
        }

        Ok(items)
    }
}

/// A source that yields items from a vector
pub struct VecSource<T> {
    items: VecDeque<T>,
}

impl<T> VecSource<T> {
    /// Create a new vector source
    pub fn new(items: Vec<T>) -> Self {
        Self {
            items: items.into(),
        }
    }

    /// Add more items to the source
    pub fn push(&mut self, item: T) {
        self.items.push_back(item);
    }

    /// Check if the source has more items
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get the number of remaining items
    pub fn len(&self) -> usize {
        self.items.len()
    }
}

#[async_trait]
impl<T: Send + 'static> Source for VecSource<T> {
    type Item = T;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut items = Vec::with_capacity(demand.min(self.items.len()));

        for _ in 0..demand {
            if let Some(item) = self.items.pop_front() {
                items.push(item);
            } else {
                break; // No more items
            }
        }

        Ok(items)
    }
}

/// A source that repeats a single value
pub struct RepeatSource<T> {
    value: T,
    remaining: Option<usize>,
}

impl<T: Clone> RepeatSource<T> {
    /// Create a source that repeats a value indefinitely
    pub fn new(value: T) -> Self {
        Self {
            value,
            remaining: None,
        }
    }

    /// Create a source that repeats a value n times
    pub fn times(value: T, count: usize) -> Self {
        Self {
            value,
            remaining: Some(count),
        }
    }
}

#[async_trait]
impl<T: Clone + Send + 'static> Source for RepeatSource<T> {
    type Item = T;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut items = Vec::with_capacity(demand);

        let actual_demand = match self.remaining {
            Some(ref mut rem) => {
                let available = *rem;
                let to_take = demand.min(available);
                *rem -= to_take;
                to_take
            }
            None => demand, // Infinite repetition
        };

        for _ in 0..actual_demand {
            items.push(self.value.clone());
        }

        Ok(items)
    }
}

/// A source that generates items at timed intervals
pub struct IntervalSource<P> {
    inner: P,
    interval: Duration,
    last_produced: Option<Instant>,
}

impl<P> IntervalSource<P> {
    /// Create a new interval source
    pub fn new(inner: P, interval: Duration) -> Self {
        Self {
            inner,
            interval,
            last_produced: None,
        }
    }
}

#[async_trait]
impl<P: Source + Send> Source for IntervalSource<P> {
    type Item = P::Item;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut items = Vec::with_capacity(demand);

        for _ in 0..demand {
            let now = Instant::now();

            if let Some(last) = self.last_produced {
                let elapsed = now.duration_since(last);
                if elapsed < self.interval {
                    sleep(self.interval - elapsed).await;
                }
            }

            let inner_items = self.inner.handle_demand(1).await?;
            if let Some(item) = inner_items.into_iter().next() {
                items.push(item);
                self.last_produced = Some(Instant::now());
            } else {
                break; // Inner source exhausted
            }
        }

        Ok(items)
    }
}

/// A source that chunks items from another source
pub struct ChunkSource<P: Source> {
    inner: P,
    chunk_size: usize,
    buffer: Vec<P::Item>,
}

impl<P: Source> ChunkSource<P> {
    /// Create a new chunk source
    pub fn new(inner: P, chunk_size: usize) -> Self {
        Self {
            inner,
            chunk_size,
            buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl<P: Source + Send> Source for ChunkSource<P> {
    type Item = Vec<P::Item>;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut chunks = Vec::with_capacity(demand);

        for _ in 0..demand {
            // Fill buffer until we have enough items or the inner source is exhausted
            while self.buffer.len() < self.chunk_size {
                let inner_items = self
                    .inner
                    .handle_demand(self.chunk_size - self.buffer.len())
                    .await?;
                if inner_items.is_empty() {
                    break; // Inner source exhausted
                }
                self.buffer.extend(inner_items);
            }

            if self.buffer.is_empty() {
                break; // No more items to chunk
            } else {
                // Take up to chunk_size items
                let chunk_len = self.chunk_size.min(self.buffer.len());
                let chunk = self.buffer.drain(..chunk_len).collect();
                chunks.push(chunk);
            }
        }

        Ok(chunks)
    }
}

/// A source that merges items from multiple sources in round-robin fashion
pub struct MergeSource<T> {
    sources: Vec<Box<dyn Source<Item = T> + Send + Sync>>,
    current: usize,
    exhausted: Vec<bool>,
}

impl<T: Send + 'static> MergeSource<T> {
    /// Create a new merge source
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            current: 0,
            exhausted: Vec::new(),
        }
    }

    /// Add a source to merge
    pub fn add_source<P>(mut self, source: P) -> Self
    where
        P: Source<Item = T> + Send + Sync + 'static,
    {
        self.sources.push(Box::new(source));
        self.exhausted.push(false);
        self
    }

    /// Check if all sources are exhausted
    fn all_exhausted(&self) -> bool {
        self.exhausted.iter().all(|&x| x)
    }
}

#[async_trait]
impl<T: Send + 'static> Source for MergeSource<T> {
    type Item = T;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        if self.sources.is_empty() || self.all_exhausted() {
            return Ok(Vec::new());
        }

        let mut items = Vec::with_capacity(demand);
        let mut attempted_sources = 0;

        while items.len() < demand && attempted_sources < self.sources.len() {
            if !self.exhausted[self.current] {
                let source_items = self.sources[self.current].handle_demand(1).await?;
                if source_items.is_empty() {
                    self.exhausted[self.current] = true;
                } else {
                    items.extend(source_items);
                    attempted_sources = 0; // Reset counter when we get items
                }
            }

            // Move to next source for round-robin
            self.current = (self.current + 1) % self.sources.len();

            // If we've cycled through without getting items, check if all exhausted
            if self.current == 0 {
                attempted_sources += 1;
                if self.all_exhausted() {
                    break;
                }
            }
        }

        Ok(items)
    }
}

impl<T: Send + 'static> Default for MergeSource<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A source that generates fibonacci numbers
pub struct FibonacciSource {
    a: u64,
    b: u64,
    count: Option<usize>,
}

impl FibonacciSource {
    /// Create an infinite fibonacci source
    pub fn new() -> Self {
        Self {
            a: 0,
            b: 1,
            count: None,
        }
    }

    /// Create a fibonacci source with a limit
    pub fn with_limit(limit: usize) -> Self {
        Self {
            a: 0,
            b: 1,
            count: Some(limit),
        }
    }
}

#[async_trait]
impl Source for FibonacciSource {
    type Item = u64;

    async fn handle_demand(&mut self, demand: usize) -> Result<Vec<Self::Item>> {
        let mut items = Vec::with_capacity(demand);

        let actual_demand = match self.count {
            Some(ref mut remaining) => {
                let available = *remaining;
                let to_take = demand.min(available);
                *remaining -= to_take;
                to_take
            }
            None => demand, // Infinite fibonacci
        };

        for _ in 0..actual_demand {
            let result = self.a;
            let next = self.a + self.b;
            self.a = self.b;
            self.b = next;
            items.push(result);
        }

        Ok(items)
    }
}

impl Default for FibonacciSource {
    fn default() -> Self {
        Self::new()
    }
}
