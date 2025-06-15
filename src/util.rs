//! Utility functions and helper types.

use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::traits::{Consumer, Processor, Producer};

/// Helper function to create a simple producer from a function
pub fn from_fn<F, Fut, T>(f: F) -> FnProducer<F, Fut, T>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Option<T>>> + Send,
    T: Send + 'static,
{
    FnProducer {
        f,
        _phantom: std::marker::PhantomData,
    }
}

/// A producer created from a function
pub struct FnProducer<F, Fut, T>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Option<T>>> + Send,
    T: Send + 'static,
{
    pub f: F,
    _phantom: std::marker::PhantomData<(Fut, T)>,
}

#[async_trait]
impl<F, Fut, T> Producer for FnProducer<F, Fut, T>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Option<T>>> + Send,
    T: Send + 'static,
{
    type Item = T;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        (self.f)().await
    }
}

/// Helper function to create a simple consumer from a function
pub fn consumer_from_fn<F, Fut, T>(f: F) -> FnConsumer<F, Fut, T>
where
    F: FnMut(T) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
    T: Send + 'static,
{
    FnConsumer {
        f,
        _phantom: std::marker::PhantomData,
    }
}

/// A consumer created from a function
pub struct FnConsumer<F, Fut, T>
where
    F: FnMut(T) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
    T: Send + 'static,
{
    pub f: F,
    _phantom: std::marker::PhantomData<(Fut, T)>,
}

#[async_trait]
impl<F, Fut, T> Consumer for FnConsumer<F, Fut, T>
where
    F: FnMut(T) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
    T: Send + 'static,
{
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        (self.f)(item).await
    }
}

/// Helper function to create a simple processor from a function
pub fn processor_from_fn<F, Fut, T, U>(f: F) -> FnProcessor<F, Fut, T, U>
where
    F: FnMut(T) -> Fut + Send,
    Fut: Future<Output = Result<Vec<U>>> + Send,
    T: Send + 'static,
    U: Send + 'static,
{
    FnProcessor {
        f,
        _phantom: std::marker::PhantomData,
    }
}

/// A processor created from a function
pub struct FnProcessor<F, Fut, T, U>
where
    F: FnMut(T) -> Fut + Send,
    Fut: Future<Output = Result<Vec<U>>> + Send,
    T: Send + 'static,
    U: Send + 'static,
{
    pub f: F,
    _phantom: std::marker::PhantomData<(Fut, T, U)>,
}

#[async_trait]
impl<F, Fut, T, U> Processor for FnProcessor<F, Fut, T, U>
where
    F: FnMut(T) -> Fut + Send,
    Fut: Future<Output = Result<Vec<U>>> + Send,
    T: Send + 'static,
    U: Send + 'static,
{
    type Input = T;
    type Output = U;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        (self.f)(item).await
    }
}

/// A cancellable future that can be interrupted
pub struct Cancellable<F> {
    future: F,
    token: CancellationToken,
}

impl<F> Cancellable<F> {
    /// Create a new cancellable future
    pub fn new(future: F, token: CancellationToken) -> Self {
        Self { future, token }
    }
}

impl<F> Future for Cancellable<F>
where
    F: Future + Unpin,
{
    type Output = Result<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Check if cancellation was requested
        if self.token.is_cancelled() {
            return Poll::Ready(Err(crate::error::Error::Shutdown));
        }

        // Poll the inner future
        match Pin::new(&mut self.future).poll(cx) {
            Poll::Ready(output) => Poll::Ready(Ok(output)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Extension trait for making futures cancellable
pub trait CancellableExt: Future + Sized {
    /// Make this future cancellable
    fn cancellable(self, token: CancellationToken) -> Cancellable<Self> {
        Cancellable::new(self, token)
    }
}

impl<F: Future> CancellableExt for F {}

/// A circuit breaker for handling failures
pub struct CircuitBreaker {
    failure_threshold: usize,
    success_threshold: usize,
    timeout: std::time::Duration,
    failure_count: usize,
    success_count: usize,
    state: CircuitBreakerState,
    last_failure: Option<std::time::Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(
        failure_threshold: usize,
        success_threshold: usize,
        timeout: std::time::Duration,
    ) -> Self {
        Self {
            failure_threshold,
            success_threshold,
            timeout,
            failure_count: 0,
            success_count: 0,
            state: CircuitBreakerState::Closed,
            last_failure: None,
        }
    }

    /// Check if the circuit breaker allows the operation
    pub fn allow(&mut self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                if let Some(last_failure) = self.last_failure {
                    if last_failure.elapsed() >= self.timeout {
                        self.state = CircuitBreakerState::HalfOpen;
                        self.success_count = 0;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitBreakerState::HalfOpen => true,
        }
    }

    /// Record a successful operation
    pub fn record_success(&mut self) {
        self.failure_count = 0;
        match self.state {
            CircuitBreakerState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= self.success_threshold {
                    self.state = CircuitBreakerState::Closed;
                }
            }
            _ => {}
        }
    }

    /// Record a failed operation
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure = Some(std::time::Instant::now());

        if self.failure_count >= self.failure_threshold {
            self.state = CircuitBreakerState::Open;
        }
    }

    /// Get the current state
    pub fn state(&self) -> &str {
        match self.state {
            CircuitBreakerState::Closed => "closed",
            CircuitBreakerState::Open => "open",
            CircuitBreakerState::HalfOpen => "half-open",
        }
    }
}

/// A retry utility with exponential backoff
pub struct RetryPolicy {
    max_attempts: usize,
    initial_delay: std::time::Duration,
    max_delay: std::time::Duration,
    backoff_factor: f64,
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: std::time::Duration::from_millis(100),
            max_delay: std::time::Duration::from_secs(60),
            backoff_factor: 2.0,
        }
    }

    /// Set the maximum number of attempts
    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the initial delay
    pub fn with_initial_delay(mut self, delay: std::time::Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set the maximum delay
    pub fn with_max_delay(mut self, delay: std::time::Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set the backoff factor
    pub fn with_backoff_factor(mut self, factor: f64) -> Self {
        self.backoff_factor = factor;
        self
    }

    /// Execute a function with retry logic
    pub async fn execute<F, Fut, T, E>(&self, mut f: F) -> std::result::Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let mut attempt = 0;
        let mut delay = self.initial_delay;

        loop {
            attempt += 1;

            match f().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if attempt >= self.max_attempts {
                        return Err(error);
                    }

                    tokio::time::sleep(delay).await;

                    delay = std::cmp::min(
                        std::time::Duration::from_millis(
                            (delay.as_millis() as f64 * self.backoff_factor) as u64,
                        ),
                        self.max_delay,
                    );
                }
            }
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics collection utilities
#[cfg(feature = "metrics")]
pub mod metrics {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    /// Simple metrics collector
    #[derive(Debug, Clone)]
    pub struct Metrics {
        produced: Arc<AtomicU64>,
        consumed: Arc<AtomicU64>,
        processed: Arc<AtomicU64>,
        errors: Arc<AtomicU64>,
        start_time: Arc<std::sync::Mutex<Option<Instant>>>,
    }

    impl Metrics {
        /// Create new metrics
        pub fn new() -> Self {
            Self {
                produced: Arc::new(AtomicU64::new(0)),
                consumed: Arc::new(AtomicU64::new(0)),
                processed: Arc::new(AtomicU64::new(0)),
                errors: Arc::new(AtomicU64::new(0)),
                start_time: Arc::new(std::sync::Mutex::new(None)),
            }
        }

        /// Record a produced item
        pub fn record_produced(&self) {
            self.produced.fetch_add(1, Ordering::Relaxed);
            self.ensure_start_time();
        }

        /// Record a consumed item
        pub fn record_consumed(&self) {
            self.consumed.fetch_add(1, Ordering::Relaxed);
            self.ensure_start_time();
        }

        /// Record a processed item
        pub fn record_processed(&self) {
            self.processed.fetch_add(1, Ordering::Relaxed);
            self.ensure_start_time();
        }

        /// Record an error
        pub fn record_error(&self) {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }

        /// Get current counts
        pub fn counts(&self) -> (u64, u64, u64, u64) {
            (
                self.produced.load(Ordering::Relaxed),
                self.consumed.load(Ordering::Relaxed),
                self.processed.load(Ordering::Relaxed),
                self.errors.load(Ordering::Relaxed),
            )
        }

        /// Get throughput metrics
        pub fn throughput(&self) -> Option<(f64, f64, f64)> {
            let start = *self.start_time.lock().unwrap()?;
            let elapsed = start.elapsed().as_secs_f64();

            if elapsed > 0.0 {
                let (produced, consumed, processed, _) = self.counts();
                Some((
                    produced as f64 / elapsed,
                    consumed as f64 / elapsed,
                    processed as f64 / elapsed,
                ))
            } else {
                None
            }
        }

        fn ensure_start_time(&self) {
            let mut start_time = self.start_time.lock().unwrap();
            if start_time.is_none() {
                *start_time = Some(Instant::now());
            }
        }
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }
}

pub struct StreamWithTimeout<S> {
    stream: S,
    _timeout: std::time::Duration,
}

impl<S> StreamWithTimeout<S> {
    pub fn new(stream: S, timeout: std::time::Duration) -> Self {
        Self {
            stream,
            _timeout: timeout,
        }
    }
}

impl<S: Stream + Send + Sync + Unpin> Stream for StreamWithTimeout<S> {
    type Item = S::Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<StreamWithTimeout<S> as Stream>::Item>> {
        // For now, just delegate to the inner stream
        // TODO: Implement timeout logic
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

pub async fn stream_into_vec<S>(stream: S) -> Vec<S::Item>
where
    S: Stream + Send + Sync + Unpin,
    S::Item: Send,
{
    let mut vec = Vec::new();
    tokio::pin!(stream);
    while let Some(item) = stream.next().await {
        vec.push(item);
    }
    vec
}

pub async fn stream_into_vec_with_timeout<S>(
    stream: S,
    timeout: std::time::Duration,
) -> Vec<S::Item>
where
    S: Stream + Send + Sync + Unpin,
    S::Item: Send,
{
    let stream_with_timeout = StreamWithTimeout::new(stream, timeout);
    stream_into_vec(stream_with_timeout).await
}

pub async fn stream_into_vec_with_timeout_and_channel<S, T>(
    stream: S,
    timeout: std::time::Duration,
    _channel: mpsc::Sender<T>,
) -> Vec<S::Item>
where
    S: Stream + Send + Sync + Unpin,
    S::Item: Send,
    T: Send + 'static + Default,
{
    let stream_with_timeout = StreamWithTimeout::new(stream, timeout);
    let mut vec = Vec::new();
    tokio::pin!(stream_with_timeout);
    while let Some(item) = stream_with_timeout.next().await {
        vec.push(item);
    }
    vec
}

pub async fn stream_into_vec_with_timeout_and_channel_and_handle<S, T>(
    stream: S,
    timeout: std::time::Duration,
    channel: mpsc::Sender<T>,
) -> (Vec<S::Item>, JoinHandle<()>)
where
    S: Stream + Send + Sync + Unpin,
    S::Item: Send,
    T: Send + 'static + Default,
{
    let stream_with_timeout = StreamWithTimeout::new(stream, timeout);
    let mut vec = Vec::new();
    tokio::pin!(stream_with_timeout);
    while let Some(item) = stream_with_timeout.next().await {
        vec.push(item);
    }
    let handle = tokio::spawn(async move {
        // Do something with the channel
        let _ = channel.send(T::default()).await;
    });
    (vec, handle)
}

pub async fn stream_into_vec_with_timeout_and_channel_and_handle_and_future<S, T, F>(
    stream: S,
    timeout: std::time::Duration,
    channel: mpsc::Sender<T>,
    future: F,
) -> (Vec<S::Item>, JoinHandle<()>)
where
    S: Stream + Send + Sync + Unpin,
    S::Item: Send,
    T: Send + 'static + Default,
    F: Future<Output = ()> + Send + 'static,
{
    let stream_with_timeout = StreamWithTimeout::new(stream, timeout);
    let mut vec = Vec::new();
    tokio::pin!(stream_with_timeout);
    while let Some(item) = stream_with_timeout.next().await {
        vec.push(item);
    }
    let handle = tokio::spawn(async move {
        // Do something with the channel and future
        let _ = channel.send(T::default()).await;
        future.await;
    });
    (vec, handle)
}
