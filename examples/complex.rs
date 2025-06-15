//! Complex pipeline examples demonstrating advanced features
//!
//! Run with: cargo run --example complex_pipeline

use std::time::Duration;
use streamweld::error::{Error, Result};
use streamweld::impls::consumers::{CollectSink, PrintSink, ThroughputSink};
use streamweld::impls::processors::{
    DelayProcessor, ErrorHandlingProcessor, FilterProcessor, MapProcessor,
};
use streamweld::impls::producers::RangeSource;
use streamweld::prelude::Pipeline;
use streamweld::traits::Processor;
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
    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        // Convert i64 to DataRecord
        let record = DataRecord {
            id: item as i64,
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
    let source = streamweld::util::from_fn(|| async {
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

    // Sink that handles both success and error cases
    let sink = streamweld::util::sink_from_fn(|result: Result<String>| async move {
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
            let _now = std::time::Instant::now();
            let _elapsed = _now.duration_since(self.last_adjustment);

            if _elapsed.as_secs() >= 1 {
                // Reset to base rate every second
                self.current_rate = self.base_rate;
                self.last_adjustment = _now;
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

    let source = RangeSource::new(1..16);
    let adaptive_limiter = AdaptiveRateLimiter::new(5); // Start with 5/sec
    let sink = PrintSink::with_prefix("Adaptive".to_string());

    let _start = std::time::Instant::now();

    Pipeline::new(source, adaptive_limiter).sink(sink).await?;

    let _elapsed = _start.elapsed();
    println!("Completed in {:.2} seconds", _elapsed.as_secs_f64());

    println!();
    Ok(())
}

/// Example 4: Pipeline with metrics and monitoring
async fn monitored_pipeline() -> Result<()> {
    println!("=== Monitored Pipeline Example ===");

    let source = RangeSource::new(1..21);
    let collector = CollectSink::new();
    let throughput_sink = ThroughputSink::new(collector.clone(), Duration::from_millis(500));

    let pipeline = Pipeline::new(source, DelayProcessor::new(Duration::from_millis(20)));
    pipeline.sink(throughput_sink).await?;

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

/// Example 5: Fan-out with multiple sinks
async fn fan_out_example() -> Result<()> {
    println!("=== Fan-Out Pipeline Example ===");

    let source = RangeSource::new(1..21);

    // Create two parallel processing paths
    let high_value_processor = FilterProcessor::new(|x: &i64| *x > 5);
    let low_value_processor = FilterProcessor::new(|x: &i64| *x <= 5);

    let high_value_sink = PrintSink::with_prefix("High-Value".to_string());
    let low_value_sink = PrintSink::with_prefix("Low-Value".to_string());

    // Create two separate pipelines
    let high_value_pipeline = Pipeline::new(RangeSource::new(1..11), high_value_processor);
    let low_value_pipeline = Pipeline::new(source, low_value_processor);

    // Run both pipelines concurrently
    let high_value_future = high_value_pipeline.sink(high_value_sink);
    let low_value_future = low_value_pipeline.sink(low_value_sink);

    let (high_result, low_result) = tokio::join!(high_value_future, low_value_future);
    high_result?;
    low_result?;

    println!();
    Ok(())
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
