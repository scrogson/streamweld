//! Processor implementations for the streamweld library.
//!
//! This module provides concrete implementations of processors that transform data
//! flowing through processing pipelines using batch-first design for efficiency.

pub mod combinators;

use async_trait::async_trait;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::core::{Processor, Result};

/// A no-op processor that passes items through unchanged
pub struct NoOpProcessor<T> {
    _phantom: PhantomData<T>,
}

impl<T> NoOpProcessor<T> {
    /// Create a new no-op processor
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for NoOpProcessor<T> {
    type Input = T;
    type Output = T;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        Ok(items) // Pass through unchanged
    }
}

impl<T> Default for NoOpProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A processor that maps items through a function
pub struct MapProcessor<F, I, O> {
    f: F,
    _phantom: PhantomData<(I, O)>,
}

impl<F, I, O> MapProcessor<F, I, O>
where
    F: FnMut(I) -> O + Send,
    I: Send + 'static,
    O: Send + 'static,
{
    /// Create a new map processor
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, I, O> Processor for MapProcessor<F, I, O>
where
    F: FnMut(I) -> O + Send,
    I: Send + 'static,
    O: Send + 'static,
{
    type Input = I;
    type Output = O;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        Ok(items.into_iter().map(&mut self.f).collect())
    }
}

/// A processor that filters items based on a predicate
pub struct FilterProcessor<F, T> {
    predicate: F,
    _phantom: PhantomData<T>,
}

impl<F, T> FilterProcessor<F, T>
where
    F: FnMut(&T) -> bool + Send,
    T: Send + 'static,
{
    /// Create a new filter processor
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> Processor for FilterProcessor<F, T>
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

/// A processor that collects items into batches
pub struct BatchProcessor<T> {
    batch_size: usize,
    batch: Vec<T>,
}

impl<T> BatchProcessor<T> {
    /// Create a new batch processor
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            batch: Vec::new(),
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for BatchProcessor<T> {
    type Input = T;
    type Output = Vec<T>;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::new();

        // Add new items to current batch
        self.batch.extend(items);

        // Emit completed batches
        while self.batch.len() >= self.batch_size {
            let batch: Vec<T> = self.batch.drain(..self.batch_size).collect();
            results.push(batch);
        }

        Ok(results)
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        if self.batch.is_empty() {
            Ok(vec![])
        } else {
            let batch = std::mem::take(&mut self.batch);
            Ok(vec![batch])
        }
    }
}

/// A processor that debatches items (flattens batches)
pub struct DebatchProcessor<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> DebatchProcessor<T> {
    /// Create a new debatch processor
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for DebatchProcessor<T> {
    type Input = Vec<T>;
    type Output = T;

    async fn process_batch(&mut self, batches: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        Ok(batches.into_iter().flatten().collect())
    }
}

impl<T> Default for DebatchProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A processor that adds delays between items
pub struct DelayProcessor<T> {
    delay: Duration,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> DelayProcessor<T> {
    /// Create a new delay processor
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for DelayProcessor<T> {
    type Input = T;
    type Output = T;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::with_capacity(items.len());

        for item in items {
            sleep(self.delay).await;
            results.push(item);
        }

        Ok(results)
    }
}

/// A processor that takes only the first N items
pub struct TakeProcessor<T> {
    remaining: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> TakeProcessor<T> {
    /// Create a new take processor
    pub fn new(count: usize) -> Self {
        Self {
            remaining: count,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for TakeProcessor<T> {
    type Input = T;
    type Output = T;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let to_take = items.len().min(self.remaining);
        self.remaining = self.remaining.saturating_sub(to_take);

        Ok(items.into_iter().take(to_take).collect())
    }
}

/// A processor that skips the first N items
pub struct SkipProcessor<T> {
    remaining: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> SkipProcessor<T> {
    /// Create a new skip processor
    pub fn new(count: usize) -> Self {
        Self {
            remaining: count,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for SkipProcessor<T> {
    type Input = T;
    type Output = T;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let to_skip = items.len().min(self.remaining);
        self.remaining = self.remaining.saturating_sub(to_skip);

        Ok(items.into_iter().skip(to_skip).collect())
    }
}

/// A processor that buffers items with size and time constraints
pub struct BufferProcessor<T> {
    max_size: usize,
    max_duration: Duration,
    buffer: VecDeque<T>,
    buffer_start: Option<Instant>,
}

impl<T> BufferProcessor<T> {
    /// Create a new buffer processor
    pub fn new(max_size: usize, max_duration: Duration) -> Self {
        Self {
            max_size,
            max_duration,
            buffer: VecDeque::new(),
            buffer_start: None,
        }
    }

    /// Check if buffer should be flushed
    fn should_flush(&self) -> bool {
        if self.buffer.len() >= self.max_size {
            return true;
        }

        if let Some(start) = self.buffer_start {
            start.elapsed() >= self.max_duration
        } else {
            false
        }
    }

    /// Flush the current buffer
    fn flush(&mut self) -> Vec<T> {
        self.buffer_start = None;
        self.buffer.drain(..).collect()
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for BufferProcessor<T> {
    type Input = T;
    type Output = Vec<T>;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::new();

        for item in items {
            if self.buffer.is_empty() {
                self.buffer_start = Some(Instant::now());
            }

            self.buffer.push_back(item);

            if self.should_flush() {
                let batch = self.flush();
                results.push(batch);
            }
        }

        Ok(results)
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        if self.buffer.is_empty() {
            Ok(vec![])
        } else {
            let batch = self.flush();
            Ok(vec![batch])
        }
    }
}

/// A processor that applies rate limiting
pub struct RateLimitProcessor<T> {
    _max_rate: u64,
    min_interval: Duration,
    last_processed: Option<Instant>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> RateLimitProcessor<T> {
    /// Create a new rate limit processor
    pub fn new(max_rate_per_second: u64) -> Self {
        let min_interval = Duration::from_nanos(1_000_000_000 / max_rate_per_second);
        Self {
            _max_rate: max_rate_per_second,
            min_interval,
            last_processed: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for RateLimitProcessor<T> {
    type Input = T;
    type Output = T;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::with_capacity(items.len());

        for item in items {
            let now = Instant::now();

            if let Some(last) = self.last_processed {
                let elapsed = now.duration_since(last);
                if elapsed < self.min_interval {
                    sleep(self.min_interval - elapsed).await;
                }
            }

            self.last_processed = Some(Instant::now());
            results.push(item);
        }

        Ok(results)
    }
}

/// A processor that transforms errors into results
pub struct ErrorHandlingProcessor<P> {
    inner: P,
}

impl<P> ErrorHandlingProcessor<P> {
    /// Create a new error handling processor
    pub fn new(inner: P) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<P: Processor + Send + Sync> Processor for ErrorHandlingProcessor<P>
where
    P::Input: Send + 'static,
    P::Output: Send + 'static,
{
    type Input = P::Input;
    type Output = Result<P::Output>;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::new();

        for item in items {
            match self.inner.process(item).await {
                Ok(outputs) => {
                    // process() returns Vec<Output>, so we need to handle each output
                    for output in outputs {
                        results.push(Ok(output));
                    }
                }
                Err(e) => results.push(Err(e)),
            }
        }

        Ok(results)
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        let outputs = self.inner.finish().await?;
        Ok(outputs.into_iter().map(Ok).collect())
    }
}
