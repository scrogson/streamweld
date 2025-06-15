//! Basic usage examples for StreamWeld
//!
//! Run with: cargo run --example basic

use std::time::Duration;
use streamweld::pipeline::Pipeline;
use streamweld::prelude::*;

/// Custom sink for Vec<i64>
struct DebugPrintSink;

#[async_trait::async_trait]
impl streamweld::core::Sink for DebugPrintSink {
    type Item = Vec<i64>;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> streamweld::core::Result<()> {
        for batch in items {
            println!("Batch: {:?}", batch);
        }
        Ok(())
    }

    async fn write(&mut self, item: Self::Item) -> streamweld::core::Result<()> {
        println!("Batch: {:?}", item);
        Ok(())
    }
}

/// Example 1: Simple number processing
async fn simple_example() -> Result<()> {
    println!("=== Simple Number Processing ===");

    let source = RangeSource::new(1..11);
    let sink = PrintSink::with_prefix("Number".to_string());

    Pipeline::new(source, NoOpProcessor::<i64>::new())
        .buffer_size(3)
        .sink(sink)
        .await?;

    println!();
    Ok(())
}

/// Example 2: Transform and filter
async fn transform_filter_example() -> Result<()> {
    println!("=== Transform and Filter ===");

    let source = RangeSource::new(1..21)
        .filter(|x| x % 3 == 0)
        .map(|x| x * x);

    Pipeline::new(source, NoOpProcessor::<i64>::new())
        .sink(PrintSink::with_prefix("Square".to_string()))
        .await?;

    println!();
    Ok(())
}

/// Example 3: Using function-based components
async fn functional_example() -> Result<()> {
    println!("=== Functional Components ===");

    // Create a source from a function
    let source = streamweld::utils::from_fn(|| async {
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

    // Create a sink from a function
    let sink = streamweld::utils::into_fn(|item: String| async move {
        println!("Processed: {}", item.to_uppercase());
        Ok(())
    });

    Pipeline::new(source, NoOpProcessor::<String>::new())
        .sink(sink)
        .await?;

    println!();
    Ok(())
}

/// Example 4: Collection and counting
async fn collection_example() -> Result<()> {
    println!("=== Collection and Counting ===");

    let source = FibonacciSource::with_limit(10);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, NoOpProcessor::<u64>::new())
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

    let source = RangeSource::new(1..9);
    let rate_limiter = RateLimitProcessor::new(3); // 3 items per second

    let start = std::time::Instant::now();

    Pipeline::new(source, rate_limiter)
        .operation_timeout(Duration::from_secs(5))
        .sink(PrintSink::with_prefix("Rate-limited".to_string()))
        .await?;

    let elapsed = start.elapsed();
    println!("Completed in {:.2} seconds", elapsed.as_secs_f64());

    println!();
    Ok(())
}

/// Example 6: Batching
async fn batching_example() -> Result<()> {
    println!("=== Batching ===");

    let source = RangeSource::new(1..16);
    let batcher = BatchProcessor::new(4); // Groups of 4

    Pipeline::new(source, batcher).sink(DebugPrintSink).await?;

    println!();
    Ok(())
}

/// Example 7: Merging sources (replaces chaining)
async fn merging_example() -> Result<()> {
    println!("=== Merging Sources ===");

    let source1 = RangeSource::new(1..4);
    let source2 = RangeSource::new(10..13);
    let source3 = RangeSource::new(20..23);

    // Use MergeSource instead of chain
    let merged = MergeSource::new()
        .add_source(source1)
        .add_source(source2)
        .add_source(source3);

    Pipeline::new(merged, NoOpProcessor::<i64>::new())
        .sink(PrintSink::with_prefix("Merged".to_string()))
        .await?;

    println!();
    Ok(())
}

/// Example 8: Using combinators
async fn combinators_example() -> Result<()> {
    println!("=== Using Combinators ===");

    let source = RangeSource::new(1..51)
        .filter(|x| x % 2 == 0) // Even numbers
        .map(|x| x * 3) // Multiply by 3
        .take(5); // Take first 5

    Pipeline::new(source, NoOpProcessor::<i64>::new())
        .sink(PrintSink::with_prefix("Combined".to_string()))
        .await?;

    println!();
    Ok(())
}

/// Example 9: GenStage-style demand processing
async fn demand_example() -> Result<()> {
    println!("=== GenStage-style Demand Processing ===");

    let mut source = RangeSource::new(1..21);

    // Demonstrate explicit demand signaling
    println!("Requesting 5 items:");
    let batch1 = source.handle_demand(5).await?;
    println!("Got: {:?}", batch1);

    println!("Requesting 3 more items:");
    let batch2 = source.handle_demand(3).await?;
    println!("Got: {:?}", batch2);

    println!("Requesting remaining items:");
    let batch3 = source.handle_demand(20).await?;
    println!("Got: {:?}", batch3);

    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("StreamWeld Basic Usage Examples\n");

    simple_example().await?;
    transform_filter_example().await?;
    functional_example().await?;
    collection_example().await?;
    rate_limit_example().await?;
    batching_example().await?;
    merging_example().await?;
    combinators_example().await?;
    demand_example().await?;

    println!("All basic examples completed successfully!");
    Ok(())
}
