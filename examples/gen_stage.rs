//! GenStage-style Demand-Driven Processing Demo
//!
//! This example demonstrates the new GenStage-inspired demand-driven architecture
//! with efficient batch processing and explicit demand signaling.

use streamweld::pipeline::Pipeline;
use streamweld::prelude::*;
use streamweld::processors::MapProcessor;
use streamweld::sinks::PrintSink;
use streamweld::sources::RangeSource;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 GenStage-Style Demand-Driven Processing Demo\n");

    // Demo 1: Basic demand signaling
    println!("📊 Demo 1: Basic Demand Signaling");
    let mut source = RangeSource::new(1..11);

    // Request 3 items at a time (GenStage-style demand)
    let batch1 = source.handle_demand(3).await?;
    println!("Batch 1 (demand=3): {:?}", batch1);

    let batch2 = source.handle_demand(5).await?;
    println!("Batch 2 (demand=5): {:?}", batch2);

    let batch3 = source.handle_demand(10).await?; // More than remaining
    println!("Batch 3 (demand=10): {:?}", batch3);
    println!();

    // Demo 2: Batch-first sink processing
    println!("📝 Demo 2: Batch-First Sink Processing");
    let mut sink = PrintSink::with_prefix("Batch".to_string());

    // Process items in batches for efficiency
    sink.write_batch(vec![100, 200, 300]).await?;
    println!();

    // Demo 3: Efficient combinator chain with demand
    println!("🔗 Demo 3: Efficient Combinator Chain");
    let source = RangeSource::new(1..21);
    let mut complex_source = source
        .filter(|x| x % 2 == 0) // Even numbers
        .map(|x| x * 3) // Multiply by 3
        .take(5); // Take first 5

    // Single demand request gets all processed items
    let results = complex_source.handle_demand(10).await?;
    println!("Filtered→Mapped→Limited: {:?}", results);
    println!();

    // Demo 4: Pipeline with configurable batch sizes
    println!("⚡ Demo 4: Pipeline with Configurable Batch Processing");
    let source = RangeSource::new(1..101);
    let processor = MapProcessor::new(|x| x * x); // Square numbers
    let sink = PrintSink::with_prefix("Squared".to_string());

    Pipeline::new(source, processor)
        .demand_batch_size(20) // Process 20 items at a time
        .sink(sink)
        .await?;

    println!("\n✅ GenStage-style processing complete!");
    println!("Key benefits:");
    println!("  • Explicit demand signaling prevents memory buildup");
    println!("  • Batch processing reduces async overhead");
    println!("  • Configurable batch sizes for different workloads");
    println!("  • Natural backpressure through demand control");

    Ok(())
}
