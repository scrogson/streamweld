//! Source implementations for the streamweld library.
//!
//! This module provides concrete implementations of sources that generate data
//! for processing pipelines.

use async_trait::async_trait;
use std::collections::VecDeque;
use std::ops::Range;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::core::{Result, Source};

/// A producer that generates numbers from a range
pub struct RangeSource {
    range: Range<i64>,
}

impl RangeSource {
    /// Create a new range producer
    pub fn new(range: Range<i64>) -> Self {
        Self { range }
    }
}

#[async_trait]
impl Source for RangeSource {
    type Item = i64;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        if let Some(item) = self.range.next() {
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }
}

/// A producer that yields items from a vector
pub struct VecSource<T> {
    items: VecDeque<T>,
}

impl<T> VecSource<T> {
    /// Create a new vector producer
    pub fn new(items: Vec<T>) -> Self {
        Self {
            items: items.into(),
        }
    }

    /// Add more items to the producer
    pub fn push(&mut self, item: T) {
        self.items.push_back(item);
    }

    /// Check if the producer has more items
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

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        Ok(self.items.pop_front())
    }
}

/// A producer that repeats a single value
pub struct RepeatSource<T> {
    value: T,
    remaining: Option<usize>,
}

impl<T: Clone> RepeatSource<T> {
    /// Create a producer that repeats a value indefinitely
    pub fn new(value: T) -> Self {
        Self {
            value,
            remaining: None,
        }
    }

    /// Create a producer that repeats a value n times
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

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        match self.remaining {
            Some(ref mut rem) => {
                if *rem == 0 {
                    return Ok(None);
                }
                *rem -= 1;
            }
            None => {}
        }
        Ok(Some(self.value.clone()))
    }
}

/// A producer that generates items at timed intervals
pub struct IntervalSource<P> {
    inner: P,
    interval: Duration,
    last_produced: Option<Instant>,
}

impl<P> IntervalSource<P> {
    /// Create a new interval producer
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

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        let now = Instant::now();

        if let Some(last) = self.last_produced {
            let elapsed = now.duration_since(last);
            if elapsed < self.interval {
                sleep(self.interval - elapsed).await;
            }
        }

        let result = self.inner.produce().await;
        self.last_produced = Some(Instant::now());
        result
    }
}

/// A producer that chunks items from another producer
pub struct ChunkSource<P: Source> {
    inner: P,
    chunk_size: usize,
    buffer: Vec<P::Item>,
}

impl<P: Source> ChunkSource<P> {
    /// Create a new chunk producer
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

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        // Fill buffer until we have enough items or the inner producer is exhausted
        while self.buffer.len() < self.chunk_size {
            match self.inner.produce().await? {
                Some(item) => self.buffer.push(item),
                None => break,
            }
        }

        if self.buffer.is_empty() {
            Ok(None)
        } else {
            // Take up to chunk_size items
            let chunk_len = self.chunk_size.min(self.buffer.len());
            let chunk = self.buffer.drain(..chunk_len).collect();
            Ok(Some(chunk))
        }
    }
}

/// A producer that merges items from multiple producers in round-robin fashion
pub struct MergeSource<T> {
    producers: Vec<Box<dyn Source<Item = T> + Send>>,
    current: usize,
    exhausted: Vec<bool>,
}

impl<T: Send + 'static> MergeSource<T> {
    /// Create a new merge producer
    pub fn new() -> Self {
        Self {
            producers: Vec::new(),
            current: 0,
            exhausted: Vec::new(),
        }
    }

    /// Add a producer to merge
    pub fn add_producer<P>(mut self, producer: P) -> Self
    where
        P: Source<Item = T> + Send + 'static,
    {
        self.producers.push(Box::new(producer));
        self.exhausted.push(false);
        self
    }

    /// Check if all producers are exhausted
    fn all_exhausted(&self) -> bool {
        self.exhausted.iter().all(|&x| x)
    }
}

#[async_trait]
impl<T: Send + 'static> Source for MergeSource<T> {
    type Item = T;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        if self.producers.is_empty() || self.all_exhausted() {
            return Ok(None);
        }

        let start_index = self.current;

        loop {
            if !self.exhausted[self.current] {
                match self.producers[self.current].produce().await? {
                    Some(item) => {
                        // Move to next producer for round-robin
                        self.current = (self.current + 1) % self.producers.len();
                        return Ok(Some(item));
                    }
                    None => {
                        self.exhausted[self.current] = true;
                    }
                }
            }

            // Move to next producer
            self.current = (self.current + 1) % self.producers.len();

            // If we've gone through all producers once, check if all are exhausted
            if self.current == start_index {
                if self.all_exhausted() {
                    return Ok(None);
                }
            }
        }
    }
}

impl<T: Send + 'static> Default for MergeSource<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A producer that generates fibonacci numbers
pub struct FibonacciSource {
    a: u64,
    b: u64,
    count: Option<usize>,
}

impl FibonacciSource {
    /// Create an infinite fibonacci producer
    pub fn new() -> Self {
        Self {
            a: 0,
            b: 1,
            count: None,
        }
    }

    /// Create a fibonacci producer with a limit
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

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        if let Some(ref mut count) = self.count {
            if *count == 0 {
                return Ok(None);
            }
            *count -= 1;
        }

        let result = self.a;
        let next = self.a + self.b;
        self.a = self.b;
        self.b = next;

        Ok(Some(result))
    }
}

impl Default for FibonacciSource {
    fn default() -> Self {
        Self::new()
    }
}
