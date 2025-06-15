//! Concrete consumer implementations.

use async_trait::async_trait;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use crate::error::Result;
use crate::traits::Consumer;
use std::fmt::Display;

/// A consumer that prints items to stdout
pub struct PrintConsumer<T> {
    prefix: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T> PrintConsumer<T> {
    /// Create a new print consumer
    pub fn new() -> Self {
        Self {
            prefix: None,
            _phantom: PhantomData,
        }
    }

    /// Create a new print consumer with a prefix
    pub fn with_prefix(prefix: String) -> Self {
        Self {
            prefix: Some(prefix),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + 'static + Display> Consumer for PrintConsumer<T> {
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        match &self.prefix {
            Some(prefix) => println!("{}: {}", prefix, item),
            None => println!("{}", item),
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<T> Default for PrintConsumer<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A consumer that collects items into a vector
pub struct CollectConsumer<T> {
    items: Arc<TokioMutex<Vec<T>>>,
}

impl<T: Send + 'static + Clone> CollectConsumer<T> {
    /// Create a new collect consumer
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
impl<T: Send + 'static + Clone> Consumer for CollectConsumer<T> {
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        let mut items = self.items.lock().await;
        items.push(item);
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<T: Send + 'static + Clone> Default for CollectConsumer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for CollectConsumer<T> {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
        }
    }
}

/// A consumer that counts items
pub struct CountConsumer<T> {
    count: Arc<TokioMutex<usize>>,
    _phantom: PhantomData<T>,
}

impl<T> CountConsumer<T> {
    /// Create a new count consumer
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
impl<T: Send + 'static> Consumer for CountConsumer<T> {
    type Item = T;

    async fn consume(&mut self, _item: Self::Item) -> Result<()> {
        let mut count = self.count.lock().await;
        *count += 1;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<T> Default for CountConsumer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for CountConsumer<T> {
    fn clone(&self) -> Self {
        Self {
            count: self.count.clone(),
            _phantom: PhantomData,
        }
    }
}

/// A consumer that writes items to a file
pub struct FileConsumer<T> {
    writer: tokio::io::BufWriter<tokio::fs::File>,
    _phantom: PhantomData<T>,
}

impl<T> FileConsumer<T> {
    /// Create a new file consumer
    pub async fn new<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let file = tokio::fs::File::create(path).await?;
        Ok(Self {
            writer: tokio::io::BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    /// Create a file consumer that appends to existing file
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
impl<T: Send + 'static + Display> Consumer for FileConsumer<T> {
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        self.writer
            .write_all(format!("{}\n", item).as_bytes())
            .await
            .map_err(|e| crate::error::Error::custom(format!("File write error: {}", e)))?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A consumer that batches items and processes them together
pub struct BatchConsumer<C, T> {
    inner: C,
    batch_size: usize,
    batch: Vec<T>,
}

impl<C, T> BatchConsumer<C, T>
where
    C: Consumer<Item = Vec<T>>,
    T: Send + 'static,
{
    /// Create a new batch consumer
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
            self.inner.consume(batch).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<C, T> Consumer for BatchConsumer<C, T>
where
    C: Consumer<Item = Vec<T>> + Send,
    T: Send + 'static,
{
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        self.batch.push(item);

        if self.batch.len() >= self.batch_size {
            self.process_batch().await?;
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

/// A consumer that measures throughput
pub struct ThroughputConsumer<C> {
    inner: C,
    start_time: Option<Instant>,
    count: usize,
    report_interval: Duration,
    last_report: Option<Instant>,
}

impl<C> ThroughputConsumer<C> {
    /// Create a new throughput consumer
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
impl<C: Consumer + Send> Consumer for ThroughputConsumer<C> {
    type Item = C::Item;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        let now = Instant::now();

        if self.start_time.is_none() {
            self.start_time = Some(now);
            self.last_report = Some(now);
        }

        self.inner.consume(item).await?;
        self.count += 1;

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

/// A consumer that applies rate limiting
pub struct RateLimitedConsumer<C> {
    inner: C,
    min_interval: Duration,
    last_consumed: Option<Instant>,
}

impl<C> RateLimitedConsumer<C> {
    /// Create a new rate limited consumer
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
impl<C: Consumer + Send> Consumer for RateLimitedConsumer<C> {
    type Item = C::Item;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        let now = Instant::now();

        if let Some(last) = self.last_consumed {
            let elapsed = now.duration_since(last);
            if elapsed < self.min_interval {
                sleep(self.min_interval - elapsed).await;
            }
        }

        self.inner.consume(item).await?;
        self.last_consumed = Some(Instant::now());
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.inner.finish().await
    }
}

/// A consumer that aggregates items by key
pub struct AggregateConsumer<K, V, F, T> {
    map: Arc<TokioMutex<HashMap<K, V>>>,
    key_fn: F,
    _phantom: PhantomData<T>,
}

impl<K, V, F, T> AggregateConsumer<K, V, F, T>
where
    K: std::hash::Hash + Eq + Send + 'static + Clone,
    V: Default + Send + 'static + Clone,
    F: Fn(&T) -> (K, V) + Send + 'static,
    T: Send + 'static,
{
    /// Create a new aggregate consumer
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
impl<K, V, F, T> Consumer for AggregateConsumer<K, V, F, T>
where
    K: std::hash::Hash + Eq + Send + 'static + Clone,
    V: Default + Send + 'static + Clone,
    F: Fn(&T) -> (K, V) + Send + 'static,
    T: Send + 'static,
{
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        let (key, value) = (self.key_fn)(&item);
        let mut map = self.map.lock().await;
        map.entry(key)
            .and_modify(|v| *v = value.clone())
            .or_insert(value);
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
