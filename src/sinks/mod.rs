//! Sink implementations for the streamweld library.
//!
//! This module provides concrete implementations of sinks that consume data
//! from processing pipelines using batch-first design for efficiency.

use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use crate::core::{Result, Sink};

/// A sink that prints items to stdout.
///
/// This sink prints items to stdout.
pub struct PrintSink<T> {
    /// The prefix to print before each item
    prefix: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T> PrintSink<T> {
    /// Create a new print sink
    pub fn new() -> Self {
        Self {
            prefix: None,
            _phantom: PhantomData,
        }
    }

    /// Create a new print sink with a prefix
    pub fn with_prefix(prefix: String) -> Self {
        Self {
            prefix: Some(prefix),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static + Display> Sink for PrintSink<T> {
    type Item = T;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        for item in items {
            match &self.prefix {
                Some(prefix) => println!("{}: {}", prefix, item),
                None => println!("{}", item),
            }
        }
        Ok(())
    }
}

impl<T> Default for PrintSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A sink that collects items into a vector.
///
/// This sink collects items into a vector.
pub struct CollectSink<T> {
    /// The vector to collect items into
    items: Arc<TokioMutex<Vec<T>>>,
}

impl<T: Send + 'static + Clone> CollectSink<T> {
    /// Create a new collect sink
    pub fn new() -> Self {
        Self {
            items: Arc::new(TokioMutex::new(Vec::new())),
        }
    }

    /// Get the collected items
    pub async fn into_items(self) -> Vec<T> {
        self.items.lock().await.clone()
    }

    /// Get a clone of the items Arc for external access
    pub fn items(&self) -> Arc<TokioMutex<Vec<T>>> {
        self.items.clone()
    }
}

#[async_trait]
impl<T: Send + 'static + Clone> Sink for CollectSink<T> {
    type Item = T;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        let mut collected = self.items.lock().await;
        collected.extend(items);
        Ok(())
    }
}

impl<T: Send + 'static + Clone> Default for CollectSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for CollectSink<T> {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
        }
    }
}

/// A sink that counts items
pub struct CountSink<T> {
    count: Arc<TokioMutex<usize>>,
    _phantom: PhantomData<T>,
}

impl<T> CountSink<T> {
    /// Create a new count sink
    pub fn new() -> Self {
        Self {
            count: Arc::new(TokioMutex::new(0)),
            _phantom: PhantomData,
        }
    }

    /// Get the current count
    pub async fn count(&self) -> usize {
        *self.count.lock().await
    }

    /// Get a reference to the count
    pub fn count_ref(&self) -> Arc<TokioMutex<usize>> {
        self.count.clone()
    }
}

#[async_trait]
impl<T: Send + 'static> Sink for CountSink<T> {
    type Item = T;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        let mut count = self.count.lock().await;
        *count += items.len();
        Ok(())
    }
}

impl<T> Default for CountSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for CountSink<T> {
    fn clone(&self) -> Self {
        Self {
            count: self.count.clone(),
            _phantom: PhantomData,
        }
    }
}

/// A sink that writes items to a file
pub struct FileSink<T> {
    writer: tokio::io::BufWriter<tokio::fs::File>,
    _phantom: PhantomData<T>,
}

impl<T> FileSink<T> {
    /// Create a new file sink
    pub async fn new<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let file = tokio::fs::File::create(path).await?;
        Ok(Self {
            writer: tokio::io::BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    /// Create a file sink that appends to existing file
    pub async fn append<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            writer: tokio::io::BufWriter::new(file),
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<T: Send + 'static + Display> Sink for FileSink<T> {
    type Item = T;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        for item in items {
            self.writer
                .write_all(format!("{}\n", item).as_bytes())
                .await
                .map_err(|e| {
                    crate::core::error::Error::custom(format!("File write error: {}", e))
                })?;
        }
        self.writer
            .flush()
            .await
            .map_err(|e| crate::core::error::Error::custom(format!("File flush error: {}", e)))?;
        Ok(())
    }
}

/// A sink that batches items and processes them together
pub struct BatchSink<C, T> {
    inner: C,
    batch_size: usize,
    batch: Vec<T>,
}

impl<C, T> BatchSink<C, T>
where
    C: Sink<Item = Vec<T>>,
    T: Send + 'static,
{
    /// Create a new batch sink
    pub fn new(inner: C, batch_size: usize) -> Self {
        Self {
            inner,
            batch_size,
            batch: Vec::new(),
        }
    }

    /// Process the current batch
    async fn process_batch(&mut self) -> Result<()> {
        if !self.batch.is_empty() {
            let batch = std::mem::take(&mut self.batch);
            self.inner.write_batch(vec![batch]).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<C, T> Sink for BatchSink<C, T>
where
    C: Sink<Item = Vec<T>> + Send,
    T: Send + 'static,
{
    type Item = T;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        for item in items {
            self.batch.push(item);

            if self.batch.len() >= self.batch_size {
                self.process_batch().await?;
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        // Process any remaining items in the batch
        self.process_batch().await?;
        self.inner.finish().await?;
        Ok(())
    }
}

/// A sink that measures throughput
pub struct ThroughputSink<C> {
    inner: C,
    start_time: Option<Instant>,
    count: usize,
    report_interval: Duration,
    last_report: Option<Instant>,
}

impl<C> ThroughputSink<C> {
    /// Create a new throughput sink
    pub fn new(inner: C, report_interval: Duration) -> Self {
        Self {
            inner,
            start_time: None,
            count: 0,
            report_interval,
            last_report: None,
        }
    }

    /// Report current throughput
    fn report_throughput(&mut self) {
        let now = Instant::now();

        if let Some(start) = self.start_time {
            let total_elapsed = now.duration_since(start);
            let total_rate = self.count as f64 / total_elapsed.as_secs_f64();

            if let Some(last_report) = self.last_report {
                // TODO: use this
                let _interval_elapsed = now.duration_since(last_report);
                println!(
                    "Throughput: {:.2} items/sec (total: {} items in {:.2}s)",
                    total_rate,
                    self.count,
                    total_elapsed.as_secs_f64()
                );
            }

            self.last_report = Some(now);
        }
    }
}

#[async_trait]
impl<C: Sink + Send> Sink for ThroughputSink<C> {
    type Item = C::Item;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        let now = Instant::now();

        if self.start_time.is_none() {
            self.start_time = Some(now);
            self.last_report = Some(now);
        }

        let batch_size = items.len();
        self.inner.write_batch(items).await?;
        self.count += batch_size;

        if let Some(last_report) = self.last_report {
            if now.duration_since(last_report) >= self.report_interval {
                self.report_throughput();
            }
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.report_throughput();
        self.inner.finish().await
    }
}

/// A sink that applies rate limiting
pub struct RateLimitedSink<C> {
    inner: C,
    min_interval: Duration,
    last_consumed: Option<Instant>,
}

impl<C> RateLimitedSink<C> {
    /// Create a new rate limited sink
    pub fn new(inner: C, max_rate_per_second: u64) -> Self {
        let min_interval = Duration::from_nanos(1_000_000_000 / max_rate_per_second);
        Self {
            inner,
            min_interval,
            last_consumed: None,
        }
    }
}

#[async_trait]
impl<C: Sink + Send> Sink for RateLimitedSink<C> {
    type Item = C::Item;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        // Apply rate limiting per item in batch
        for item in items {
            let now = Instant::now();

            if let Some(last) = self.last_consumed {
                let elapsed = now.duration_since(last);
                if elapsed < self.min_interval {
                    sleep(self.min_interval - elapsed).await;
                }
            }

            self.inner.write(item).await?;
            self.last_consumed = Some(Instant::now());
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.inner.finish().await
    }
}

/// A sink that aggregates items by key
pub struct AggregateSink<K, V, F, T> {
    map: Arc<TokioMutex<HashMap<K, V>>>,
    key_fn: F,
    _phantom: PhantomData<T>,
}

impl<K, V, F, T> AggregateSink<K, V, F, T>
where
    K: std::hash::Hash + Eq + Send + 'static + Clone,
    V: Default + Send + 'static + Clone,
    F: Fn(&T) -> (K, V) + Send + 'static,
    T: Send + 'static,
{
    /// Create a new aggregate sink
    pub fn new(key_fn: F) -> Self {
        Self {
            map: Arc::new(TokioMutex::new(HashMap::new())),
            key_fn,
            _phantom: PhantomData,
        }
    }

    /// Get a reference to the aggregated values
    pub async fn aggregates(&self) -> HashMap<K, V> {
        let map = self.map.lock().await;
        map.clone()
    }

    /// Take ownership of the aggregated values
    pub async fn into_aggregates(self) -> HashMap<K, V> {
        let map = self.map.lock().await;
        map.clone()
    }
}

#[async_trait]
impl<K, V, F, T> Sink for AggregateSink<K, V, F, T>
where
    K: std::hash::Hash + Eq + Send + 'static + Clone,
    V: Default + Send + 'static + Clone,
    F: Fn(&T) -> (K, V) + Send + 'static,
    T: Send + 'static,
{
    type Item = T;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        let mut map = self.map.lock().await;
        for item in items {
            let (key, value) = (self.key_fn)(&item);
            map.entry(key)
                .and_modify(|v| *v = value.clone())
                .or_insert(value);
        }
        Ok(())
    }
}
