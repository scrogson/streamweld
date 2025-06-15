//! Complex pipeline examples demonstrating advanced features
//!
//! Run with: cargo run --example complex_pipeline

use std::time::Duration;
use streamweld::core::{Error, Processor, Result};
use streamweld::pipeline::Pipeline;
use streamweld::processors::{ErrorHandlingProcessor, MapProcessor};
use streamweld::sinks::{PrintSink, ThroughputSink};
use streamweld::sources::RangeSource;
// use rand::distributions::uniform::SampleUniform; // Uncomment if needed
// extern crate rand;

/// Simulate a data record for processing
#[derive(Debug, Clone)]
struct DataRecord {
    id: i64,
    value: f64,
    category: String,
    #[allow(dead_code)]
    timestamp: std::time::SystemTime,
}

impl DataRecord {
    fn new(id: i64, value: f64, category: &str) -> Self {
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

/// A processor that validates and enriches data
struct ValidationProcessor;

#[async_trait::async_trait]
impl Processor for ValidationProcessor {
    type Input = DataRecord;
    type Output = DataRecord;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::new();

        for item in items {
            // Validate the record
            if item.value < 0.0 {
                return Err(Error::custom("Invalid negative value"));
            }

            // Enrich with additional data
            let enriched =
                DataRecord::new(item.id, item.value, &format!("{}_validated", item.category));
            results.push(enriched);
        }

        Ok(results)
    }

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        // Validate the record
        if item.value < 0.0 {
            return Err(Error::custom("Invalid negative value"));
        }

        // Enrich with additional data
        let enriched =
            DataRecord::new(item.id, item.value, &format!("{}_validated", item.category));

        Ok(vec![enriched])
    }
}

/// A processor that aggregates data
struct AggregationProcessor {
    batch_size: usize,
    current_batch: Vec<DataRecord>,
}

impl AggregationProcessor {
    fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            current_batch: Vec::with_capacity(batch_size),
        }
    }
}

#[derive(Debug)]
struct AggregatedData {
    batch_id: u64,
    records: Vec<DataRecord>,
    total_value: f64,
}

impl std::fmt::Display for AggregatedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Batch(id={}, count={}, total={:.2})",
            self.batch_id,
            self.records.len(),
            self.total_value
        )
    }
}

impl AggregationProcessor {
    fn create_aggregated(&self, batch_id: u64) -> AggregatedData {
        let total_value = self.current_batch.iter().map(|r| r.value).sum();
        AggregatedData {
            batch_id,
            records: self.current_batch.clone(),
            total_value,
        }
    }
}

#[async_trait::async_trait]
impl Processor for AggregationProcessor {
    type Input = DataRecord;
    type Output = AggregatedData;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::new();

        for item in items {
            self.current_batch.push(item);

            if self.current_batch.len() >= self.batch_size {
                let batch_id = self.current_batch[0].id;
                let aggregated = self.create_aggregated(batch_id.try_into().unwrap());
                self.current_batch.clear();
                results.push(aggregated);
            }
        }

        Ok(results)
    }

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        self.current_batch.push(item);

        if self.current_batch.len() >= self.batch_size {
            let batch_id = self.current_batch[0].id;
            let aggregated = self.create_aggregated(batch_id.try_into().unwrap());
            self.current_batch.clear();
            Ok(vec![aggregated])
        } else {
            Ok(vec![])
        }
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        if !self.current_batch.is_empty() {
            let batch_id = self.current_batch[0].id;
            let aggregated = self.create_aggregated(batch_id.try_into().unwrap());
            self.current_batch.clear();
            Ok(vec![aggregated])
        } else {
            Ok(vec![])
        }
    }
}

/// A processor that combines validation and aggregation
struct CombinedProcessor {
    #[allow(dead_code)]
    validator: ValidationProcessor,
    #[allow(dead_code)]
    aggregator: AggregationProcessor,
}

#[async_trait::async_trait]
impl Processor for CombinedProcessor {
    type Input = i64;
    type Output = DataRecord;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::new();

        for item in items {
            // Convert i64 to DataRecord
            let record = DataRecord {
                id: item,
                value: item as f64,
                timestamp: std::time::SystemTime::now(),
                category: "default".to_string(),
            };
            results.push(record);
        }

        Ok(results)
    }

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        // Convert i64 to DataRecord
        let record = DataRecord {
            id: item,
            value: item as f64,
            timestamp: std::time::SystemTime::now(),
            category: "default".to_string(),
        };
        // Return a single DataRecord
        Ok(vec![record])
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        Ok(vec![])
    }
}

/// Example 1: Multi-stage data processing pipeline
async fn data_processing_pipeline() -> Result<()> {
    println!("=== Data Processing Pipeline ===");

    let source = RangeSource::new(1..51);
    let validator = ValidationProcessor;
    let aggregator = AggregationProcessor::new(8);
    let sink = PrintSink::with_prefix("Aggregated".to_string());

    let processor = CombinedProcessor {
        validator,
        aggregator,
    };
    let pipeline = Pipeline::new(source, processor)
        .buffer_size(10)
        .operation_timeout(Duration::from_secs(10));
    pipeline.sink(sink).await?;

    println!();
    Ok(())
}

/// Example 2: Concurrent processing with error handling
async fn concurrent_with_error_handling() -> Result<()> {
    println!("=== Concurrent Processing with Error Handling ===");

    // Source that occasionally fails
    let source = streamweld::utils::from_fn(|| async {
        use std::sync::atomic::{AtomicI32, Ordering};
        static COUNTER: AtomicI32 = AtomicI32::new(0);

        let count = COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        if count > 20 {
            return Ok(None);
        }

        // Always return Ok, but the data itself can represent success or failure
        Ok(Some(count))
    });

    // Error handling processor that simulates failures
    let error_handler =
        ErrorHandlingProcessor::new(streamweld::utils::processor_from_fn(|x: i32| async move {
            // Simulate occasional failures
            if x % 7 == 0 {
                return Err(Error::custom(format!("Simulated error at item {}", x)));
            }
            Ok(vec![format!("Processed: {}", x * 2)])
        }));

    // Sink that handles both success and error cases
    let sink = streamweld::utils::into_fn(|result: Result<String>| async move {
        match result {
            Ok(value) => println!("✅ {}", value),
            Err(e) => println!("❌ Error: {}", e),
        }
        Ok(())
    });

    let pipeline = Pipeline::new(source, error_handler)
        .fail_fast(false) // Continue processing on errors
        .max_concurrency(3);
    pipeline.sink(sink).await?;

    println!();
    Ok(())
}

/// Example 3: Adaptive rate limiting
async fn adaptive_rate_limiting() -> Result<()> {
    println!("=== Adaptive Rate Limiting ===");

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
            if now.duration_since(self.last_adjustment) > Duration::from_secs(2) {
                // Simulate adaptive behavior
                self.current_rate = (self.current_rate + self.base_rate) / 2;
                self.last_adjustment = now;
            }
        }
    }

    #[async_trait::async_trait]
    impl Processor for AdaptiveRateLimiter {
        type Input = i64;
        type Output = i64;

        async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
            let mut results = Vec::new();

            for item in items {
                self.adjust_rate();

                // Simulate rate limiting delay
                let delay = Duration::from_millis(1000 / self.current_rate);
                tokio::time::sleep(delay).await;

                results.push(item);
            }

            Ok(results)
        }

        async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
            self.adjust_rate();

            // Simulate rate limiting delay
            let delay = Duration::from_millis(1000 / self.current_rate);
            tokio::time::sleep(delay).await;

            println!(
                "Rate limited item {} at {} items/sec",
                item, self.current_rate
            );
            Ok(vec![item])
        }
    }

    let source = RangeSource::new(1..11);
    let rate_limiter = AdaptiveRateLimiter::new(2);
    let sink = PrintSink::with_prefix("Rate-limited".to_string());

    Pipeline::new(source, rate_limiter).sink(sink).await?;

    println!();
    Ok(())
}

/// Example 4: Monitored pipeline with throughput tracking
async fn monitored_pipeline() -> Result<()> {
    println!("=== Monitored Pipeline ===");

    let source = RangeSource::new(1..101);
    let processor = MapProcessor::new(|x: i64| x * x);
    let throughput_sink = ThroughputSink::new(
        PrintSink::with_prefix("Squared".to_string()),
        Duration::from_secs(1),
    );

    Pipeline::new(source, processor)
        .buffer_size(20)
        .sink(throughput_sink)
        .await?;

    println!();
    Ok(())
}

/// Example 5: Fan-out processing
async fn fan_out_example() -> Result<()> {
    println!("=== Fan-out Processing ===");

    let source = RangeSource::new(1..21);
    let processor = MapProcessor::new(|x: i64| x);

    // Create multiple sinks for fan-out
    let print_sink = PrintSink::with_prefix("Print".to_string());

    // For this example, we'll just use one sink
    // In a real scenario, you'd use a dispatcher or multiple pipelines
    Pipeline::new(source, processor).sink(print_sink).await?;

    println!("Fan-out processing completed");

    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("StreamWeld Complex Pipeline Examples\n");

    data_processing_pipeline().await?;
    concurrent_with_error_handling().await?;
    adaptive_rate_limiting().await?;
    monitored_pipeline().await?;
    fan_out_example().await?;

    println!("All complex examples completed successfully!");
    Ok(())
}
