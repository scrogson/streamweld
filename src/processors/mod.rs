//! Processor implementations for the streamweld library.
//!
//! This module provides concrete implementations of processors that transform data
//! flowing through processing pipelines.

pub mod combinators;

use async_trait::async_trait;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::core::{Processor, Result};

/// A processor that maps events using a function.
///
/// This processor applies a function to each event.
pub struct MapProcessor<F, T, U> {
    f: F,
    _phantom: std::marker::PhantomData<(T, U)>,
}

impl<F, T, U> MapProcessor<F, T, U> {
    /// Create a new map processor
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, T, U> Processor for MapProcessor<F, T, U>
where
    F: FnMut(T) -> U + Send + Sync + 'static,
    T: Send + 'static,
    U: Send + 'static,
{
    type Input = T;
    type Output = U;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        Ok(vec![(self.f)(item)])
    }
}

/// A processor that filters events using a predicate.
///
/// This processor only passes events that satisfy the predicate.
pub struct FilterProcessor<F, T> {
    predicate: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<F, T> FilterProcessor<F, T> {
    /// Create a new filter processor
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> Processor for FilterProcessor<F, T>
where
    F: FnMut(&T) -> bool + Send + Sync + 'static,
    T: Send + 'static,
{
    type Input = T;
    type Output = T;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        if (self.predicate)(&item) {
            Ok(vec![item])
        } else {
            Ok(vec![])
        }
    }
}

/// A processor that batches events.
///
/// This processor collects events into batches.
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

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        self.batch.push(item);

        if self.batch.len() >= self.batch_size {
            let batch = std::mem::take(&mut self.batch);
            Ok(vec![batch])
        } else {
            Ok(vec![])
        }
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

/// A processor that debatches items
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

    async fn process(&mut self, items: Self::Input) -> Result<Vec<Self::Output>> {
        Ok(items)
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

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        sleep(self.delay).await;
        Ok(vec![item])
    }
}

/// A processor that duplicates items
pub struct DuplicateProcessor<T> {
    count: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> DuplicateProcessor<T> {
    /// Create a new duplicate processor
    pub fn new(count: usize) -> Self {
        Self {
            count,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Clone + Send + 'static> Processor for DuplicateProcessor<T> {
    type Input = T;
    type Output = T;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        Ok(vec![item; self.count])
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

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Ok(vec![item])
        } else {
            Ok(vec![])
        }
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

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Ok(vec![])
        } else {
            Ok(vec![item])
        }
    }
}

/// A processor that buffers items and releases them based on time or count
pub struct BufferProcessor<T> {
    buffer: VecDeque<T>,
    max_size: usize,
    max_age: Duration,
    buffer_start: Option<Instant>,
}

impl<T> BufferProcessor<T> {
    /// Create a new buffer processor
    pub fn new(max_size: usize, max_age: Duration) -> Self {
        Self {
            buffer: VecDeque::new(),
            max_size,
            max_age,
            buffer_start: None,
        }
    }

    /// Check if the buffer should be flushed
    fn should_flush(&self) -> bool {
        if self.buffer.len() >= self.max_size {
            return true;
        }

        if let Some(start) = self.buffer_start {
            if start.elapsed() >= self.max_age {
                return true;
            }
        }

        false
    }

    /// Flush the buffer
    fn flush(&mut self) -> Vec<T> {
        let items = self.buffer.drain(..).collect();
        self.buffer_start = None;
        items
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for BufferProcessor<T> {
    type Input = T;
    type Output = Vec<T>;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        if self.buffer.is_empty() {
            self.buffer_start = Some(Instant::now());
        }

        self.buffer.push_back(item);

        if self.should_flush() {
            let items = self.flush();
            Ok(vec![items])
        } else {
            Ok(vec![])
        }
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        if self.buffer.is_empty() {
            Ok(vec![])
        } else {
            let items = self.flush();
            Ok(vec![items])
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

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        let now = Instant::now();

        if let Some(last) = self.last_processed {
            let elapsed = now.duration_since(last);
            if elapsed < self.min_interval {
                sleep(self.min_interval - elapsed).await;
            }
        }

        self.last_processed = Some(Instant::now());
        Ok(vec![item])
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

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        match self.inner.process(item).await {
            Ok(outputs) => Ok(outputs.into_iter().map(Ok).collect()),
            Err(e) => Ok(vec![Err(e)]),
        }
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        let outputs = self.inner.finish().await?;
        Ok(outputs.into_iter().map(Ok).collect())
    }
}

/// A processor that passes through items unchanged
pub struct NoOpProcessor<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> NoOpProcessor<T> {
    /// Create a new no-op processor
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Processor for NoOpProcessor<T> {
    type Input = T;
    type Output = T;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        Ok(vec![item])
    }
}

impl<T> Default for NoOpProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}
