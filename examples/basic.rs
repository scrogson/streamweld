//! Basic usage examples for GenStage-Rust
//!
//! Run with: cargo run --example basic_usage

use std::time::Duration;
use streamweld::prelude::*;

/// Custom consumer for Vec<i64>
struct DebugPrintConsumer;

#[async_trait::async_trait]
impl streamweld::traits::Consumer for DebugPrintConsumer {
    type Item = Vec<i64>;

    async fn consume(&mut self, item: Self::Item) -> streamweld::error::Result<()> {
        println!("Batch: {:?}", item);
        Ok(())
    }
}

/// Example 1: Simple number processing
async fn simple_example() -> Result<()> {
    println!("=== Simple Number Processing ===");

    let producer = RangeProducer::new(1..11);
    let consumer = PrintConsumer::with_prefix("Number".to_string());

    Pipeline::new(producer, NoOpProcessor::<i64>::new())
        .buffer_size(3)
        .sink(consumer)
        .await?;

    println!();
    Ok(())
}

/// Example 2: Transform and filter
async fn transform_filter_example() -> Result<()> {
    println!("=== Transform and Filter ===");

    let producer = RangeProducer::new(1..21)
        .filter(|x| x % 3 == 0)
        .map(|x| x * x);

    Pipeline::new(producer, NoOpProcessor::<i64>::new())
        .sink(PrintConsumer::with_prefix("Square".to_string()))
        .await?;

    println!();
    Ok(())
}

/// Example 3: Using function-based components
async fn functional_example() -> Result<()> {
    println!("=== Functional Components ===");

    // Create a producer from a function
    let producer = streamweld::util::from_fn(|| async {
        static mut COUNTER: i32 = 0;
        unsafe {
            COUNTER += 1;
            if COUNTER <= 5 {
                Ok(Some(format!("Item-{}", COUNTER)))
            } else {
                Ok(None)
            }
        }
    });

    // Create a consumer from a function
    let consumer = streamweld::util::consumer_from_fn(|item: String| async move {
        println!("Processed: {}", item.to_uppercase());
        Ok(())
    });

    Pipeline::new(producer, NoOpProcessor::<String>::new())
        .sink(consumer)
        .await?;

    println!();
    Ok(())
}

/// Example 4: Collection and counting
async fn collection_example() -> Result<()> {
    println!("=== Collection and Counting ===");

    let producer = FibonacciProducer::with_limit(10);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, NoOpProcessor::<u64>::new())
        .sink(collector)
        .await?;

    let items = collector_ref.into_items().await;
    println!("Collected Fibonacci numbers: {:?}", items);

    println!();
    Ok(())
}

/// Example 5: Rate limiting
async fn rate_limit_example() -> Result<()> {
    println!("=== Rate Limiting ===");

    let producer = RangeProducer::new(1..9);
    let rate_limiter = RateLimitProcessor::new(3); // 3 items per second

    let start = std::time::Instant::now();

    Pipeline::new(producer, rate_limiter)
        .operation_timeout(Duration::from_secs(5))
        .sink(PrintConsumer::with_prefix("Rate-limited".to_string()))
        .await?;

    let elapsed = start.elapsed();
    println!("Completed in {:.2} seconds", elapsed.as_secs_f64());

    println!();
    Ok(())
}

/// Example 6: Batching
async fn batching_example() -> Result<()> {
    println!("=== Batching ===");

    let producer = RangeProducer::new(1..16);
    let batcher = BatchProcessor::new(4); // Groups of 4

    Pipeline::new(producer, batcher)
        .sink(DebugPrintConsumer)
        .await?;

    println!();
    Ok(())
}

/// Example 7: Chaining producers
async fn chaining_example() -> Result<()> {
    println!("=== Chaining Producers ===");

    let producer1 = RangeProducer::new(1..4);
    let producer2 = RangeProducer::new(10..13);
    let producer3 = RangeProducer::new(20..23);

    let chained = producer1.chain(producer2).chain(producer3);

    Pipeline::new(chained, NoOpProcessor::<i64>::new())
        .sink(PrintConsumer::with_prefix("Chained".to_string()))
        .await?;

    println!();
    Ok(())
}

/// Example 8: Using combinators
async fn combinators_example() -> Result<()> {
    println!("=== Using Combinators ===");

    let producer = RangeProducer::new(1..51)
        .filter(|x| x % 2 == 0) // Even numbers
        .map(|x| x * 3) // Multiply by 3
        .take(5); // Take first 5

    Pipeline::new(producer, NoOpProcessor::<i64>::new())
        .sink(PrintConsumer::with_prefix("Combined".to_string()))
        .await?;

    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("GenStage-Rust Basic Usage Examples\n");

    simple_example().await?;
    transform_filter_example().await?;
    functional_example().await?;
    collection_example().await?;
    rate_limit_example().await?;
    batching_example().await?;
    chaining_example().await?;
    combinators_example().await?;

    println!("All basic examples completed successfully!");
    Ok(())
}
