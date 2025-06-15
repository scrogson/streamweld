//! Complex pipeline examples demonstrating advanced features
//!
//! Run with: cargo run --example complex_pipeline

use std::time::Duration;
use streamweld::pipeline::PipelineStage;
use streamweld::prelude::*;
use tokio::time::sleep;

/// Simulate a data record for processing
#[derive(Debug, Clone)]
struct DataRecord {
    id: u64,
    value: f64,
    category: String,
    timestamp: std::time::SystemTime,
}

impl DataRecord {
    fn new(id: u64, value: f64, category: &str) -> Self {
        Self {
            id,
            value,
            category: category.to_string(),
            timestamp: std::time::SystemTime::now(),
        }
    }
}

impl std::fmt::Display for DataRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Record(id={}, value={:.2}, category={})",
            self.id, self.value, self.category
        )
    }
}

/// A producer that generates sample data records
#[derive(Clone)]
struct DataProducer {
    count: u64,
    max_count: u64,
}

impl DataProducer {
    fn new(max_count: u64) -> Self {
        Self {
            count: 0,
            max_count,
        }
    }
}

#[async_trait::async_trait]
impl Producer for DataProducer {
    type Item = DataRecord;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        if self.count >= self.max_count {
            return Ok(None);
        }

        self.count += 1;

        // Simulate some variety in the data
        let category = match self.count % 4 {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D",
        };

        let value = (self.count as f64) * 1.5 + (self.count % 10) as f64;

        // Simulate some production delay
        sleep(Duration::from_millis(10)).await;

        Ok(Some(DataRecord::new(self.count, value, category)))
    }
}

/// A processor that validates and enriches data
struct ValidationProcessor;

#[async_trait::async_trait]
impl Processor for ValidationProcessor {
    type Input = DataRecord;
    type Output = DataRecord;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        // Simulate validation - reject records with value > 100
        if item.value > 100.0 {
            return Ok(vec![]); // Filter out invalid records
        }

        // Enrich the data
        let mut enriched = item;
        enriched.value = enriched.value * 1.1; // Apply 10% markup

        Ok(vec![enriched])
    }
}

/// A processor that categorizes and aggregates data
struct AggregationProcessor {
    batch_size: usize,
    current_batch: Vec<DataRecord>,
}

impl AggregationProcessor {
    fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            current_batch: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct AggregatedData {
    category: String,
    count: usize,
    total_value: f64,
    avg_value: f64,
    min_id: u64,
    max_id: u64,
}

impl std::fmt::Display for AggregatedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Agg(cat={}, count={}, avg={:.2}, range={}..{})",
            self.category, self.count, self.avg_value, self.min_id, self.max_id
        )
    }
}

#[async_trait::async_trait]
impl Processor for AggregationProcessor {
    type Input = DataRecord;
    type Output = AggregatedData;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        self.current_batch.push(item);

        if self.current_batch.len() >= self.batch_size {
            let batch = std::mem::take(&mut self.current_batch);
            Ok(vec![self.aggregate_batch(batch)])
        } else {
            Ok(vec![])
        }
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        if self.current_batch.is_empty() {
            Ok(vec![])
        } else {
            let batch = std::mem::take(&mut self.current_batch);
            Ok(vec![self.aggregate_batch(batch)])
        }
    }
}

impl AggregationProcessor {
    fn aggregate_batch(&self, batch: Vec<DataRecord>) -> AggregatedData {
        let category = batch.first().unwrap().category.clone();
        let count = batch.len();
        let total_value: f64 = batch.iter().map(|r| r.value).sum();
        let avg_value = total_value / count as f64;
        let min_id = batch.iter().map(|r| r.id).min().unwrap_or(0);
        let max_id = batch.iter().map(|r| r.id).max().unwrap_or(0);

        AggregatedData {
            category,
            count,
            total_value,
            avg_value,
            min_id,
            max_id,
        }
    }
}

/// A processor that combines validation and aggregation
struct CombinedProcessor {
    validator: ValidationProcessor,
    aggregator: AggregationProcessor,
}

#[async_trait::async_trait]
impl Processor for CombinedProcessor {
    type Input = DataRecord;
    type Output = AggregatedData;
    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        // Validate
        let mut validated = self.validator.process(item).await?;
        let mut results = Vec::new();
        for v in validated.drain(..) {
            // Aggregate
            let mut agg = self.aggregator.process(v).await?;
            results.append(&mut agg);
        }
        Ok(results)
    }
    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        self.aggregator.finish().await
    }
}

/// Example 1: Multi-stage data processing pipeline
async fn data_processing_pipeline() -> Result<()> {
    println!("=== Data Processing Pipeline ===");

    let producer = DataProducer::new(50);
    let validator = ValidationProcessor;
    let aggregator = AggregationProcessor::new(8);
    let consumer = PrintConsumer::with_prefix("Aggregated".to_string());

    let processor = CombinedProcessor {
        validator,
        aggregator,
    };
    let pipeline = Pipeline::new(producer, processor)
        .buffer_size(10)
        .operation_timeout(Duration::from_secs(10));
    pipeline.sink(consumer).await?;

    println!();
    Ok(())
}

/// Example 2: Concurrent processing with error handling
async fn concurrent_with_error_handling() -> Result<()> {
    println!("=== Concurrent Processing with Error Handling ===");

    // Producer that occasionally fails
    let producer = streamweld::util::from_fn(|| async {
        static mut COUNTER: i32 = 0;
        unsafe {
            COUNTER += 1;
            if COUNTER > 20 {
                return Ok(None);
            }

            // Simulate occasional failures
            if COUNTER % 7 == 0 {
                return Err(Error::custom(format!(
                    "Simulated error at item {}",
                    COUNTER
                )));
            }

            Ok(Some(COUNTER))
        }
    });

    // Error handling processor
    let error_handler =
        ErrorHandlingProcessor::new(MapProcessor::new(|x: i32| format!("Processed: {}", x * 2)));

    // Consumer that handles both success and error cases
    let consumer = streamweld::util::consumer_from_fn(|result: Result<String>| async move {
        match result {
            Ok(value) => println!("✅ {}", value),
            Err(e) => println!("❌ Error: {}", e),
        }
        Ok(())
    });

    let pipeline = Pipeline::new(producer, error_handler)
        .fail_fast(false) // Continue processing on errors
        .max_concurrency(3);
    pipeline.sink(consumer).await?;

    println!();
    Ok(())
}

/// Example 3: Dynamic rate limiting based on system load
async fn adaptive_rate_limiting() -> Result<()> {
    println!("=== Adaptive Rate Limiting ===");

    // Simulate a system load-aware rate limiter
    struct AdaptiveRateLimiter {
        base_rate: u64,
        current_rate: u64,
        last_adjustment: std::time::Instant,
    }

    impl AdaptiveRateLimiter {
        fn new(base_rate: u64) -> Self {
            Self {
                base_rate,
                current_rate: base_rate,
                last_adjustment: std::time::Instant::now(),
            }
        }

        fn adjust_rate(&mut self) {
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(self.last_adjustment);

            if elapsed.as_secs() >= 1 {
                // Reset to base rate every second
                self.current_rate = self.base_rate;
                self.last_adjustment = now;
            }
        }
    }

    #[async_trait::async_trait]
    impl Processor for AdaptiveRateLimiter {
        type Input = i64;
        type Output = i64;

        async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
            self.adjust_rate();

            // Simulate rate limiting
            if self.current_rate > 0 {
                self.current_rate -= 1;
                Ok(vec![item])
            } else {
                Ok(vec![])
            }
        }
    }

    let producer = RangeProducer::new(1..16);
    let adaptive_limiter = AdaptiveRateLimiter::new(5); // Start with 5/sec
    let consumer = PrintConsumer::with_prefix("Adaptive".to_string());

    let start = std::time::Instant::now();

    Pipeline::new(producer, adaptive_limiter)
        .sink(consumer)
        .await?;

    let elapsed = start.elapsed();
    println!("Completed in {:.2} seconds", elapsed.as_secs_f64());

    println!();
    Ok(())
}

/// Example 4: Pipeline with metrics and monitoring
async fn monitored_pipeline() -> Result<()> {
    println!("=== Monitored Pipeline Example ===");

    let producer = RangeProducer::new(1..21);
    let collector = CollectConsumer::new();
    let throughput_consumer =
        ThroughputConsumer::new(collector.clone(), Duration::from_millis(500));

    let pipeline = Pipeline::new(producer, DelayProcessor::new(Duration::from_millis(20)));
    pipeline.sink(throughput_consumer).await?;

    // Print final statistics
    let items_arc = collector.items();
    let items_guard = items_arc.lock().await;
    let avg = if !items_guard.is_empty() {
        items_guard.iter().map(|&x| x as f64).sum::<f64>() / items_guard.len() as f64
    } else {
        0.0
    };
    println!("Total items processed: {}", items_guard.len());
    println!("Average value: {:.2}", avg);

    println!();
    Ok(())
}

/// Example 5: Fan-out with multiple consumers
async fn fan_out_example() -> Result<()> {
    println!("=== Fan-Out Pipeline Example ===");

    let producer = DataProducer::new(20);

    // Create two parallel processing paths
    let high_value_processor = FilterProcessor::new(|record: &DataRecord| record.value > 5.0);
    let low_value_processor = FilterProcessor::new(|record: &DataRecord| record.value <= 5.0);

    let high_value_consumer = PrintConsumer::with_prefix("High-Value".to_string());
    let low_value_consumer = PrintConsumer::with_prefix("Low-Value".to_string());

    // Create two separate pipelines
    let high_value_pipeline = Pipeline::new(producer.clone(), high_value_processor);
    let low_value_pipeline = Pipeline::new(producer, low_value_processor);

    // Run both pipelines concurrently
    let high_value_future = high_value_pipeline.sink(high_value_consumer);
    let low_value_future = low_value_pipeline.sink(low_value_consumer);

    tokio::try_join!(high_value_future, low_value_future)?;

    println!();
    Ok(())
}

/// Add a simple random number generator for the adaptive rate limiting example
mod rand {
    use std::cell::Cell;

    thread_local! {
        static SEED: Cell<u64> = Cell::new(1);
    }

    pub fn random<T>() -> T
    where
        T: From<f64>,
    {
        SEED.with(|seed| {
            let mut s = seed.get();
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            seed.set(s);

            T::from((s as f64) / (u64::MAX as f64))
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("GenStage-Rust Complex Pipeline Examples\n");

    data_processing_pipeline().await?;
    concurrent_with_error_handling().await?;
    adaptive_rate_limiting().await?;
    monitored_pipeline().await?;
    fan_out_example().await?;

    println!("All complex examples completed successfully!");
    Ok(())
}
