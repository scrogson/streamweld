use std::time::{Duration, Instant};
use streamweld::prelude::*;
use streamweld::utils::sink_from_fn;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Understanding Concurrency vs Dispatchers in StreamWeld\n");

    // ============================================================================
    // PART 1: CONCURRENT PIPELINE
    // This is about running SOURCE and SINK in parallel (concurrency)
    //
    // Mermaid Diagram:
    // ```mermaid
    // graph TD
    //     A["Source<br/>(Fast source)"] --> B["Buffer<br/>(Channel)"]
    //     B --> C["Sink<br/>(Slow sink)"]
    //
    //     subgraph "Async Task 1"
    //         A
    //     end
    //
    //     subgraph "Async Task 2"
    //         C
    //     end
    // ```
    // ============================================================================

    println!("=== 1. CONCURRENT PIPELINE (Source ↔ Sink Concurrency) ===");
    println!("This runs the source and sink in separate async tasks with a buffer between them.");
    println!("┌─────────┐    ┌────────┐    ┌──────────┐");
    println!("│ Source  │───▶│ Buffer │───▶│   Sink   │");
    println!("│(Task 1) │    │(Channel)│    │ (Task 2) │");
    println!("└─────────┘    └────────┘    └──────────┘\n");

    let start = Instant::now();

    // Create a slow sink that takes time to process each item
    let slow_sink = sink_from_fn(|item: i64| async move {
        println!("  🐌 Processing item {} (takes 100ms)", item);
        sleep(Duration::from_millis(100)).await;
        Ok(())
    });

    // Use ConcurrentPipeline - source and sink run in parallel
    let source = RangeSource::new(1..6);
    let pipeline = ConcurrentPipeline::new(source, slow_sink).buffer_size(3); // Buffer up to 3 items between source and sink

    pipeline.run().await?;

    let elapsed = start.elapsed();
    println!("⏱️  Concurrent pipeline took: {:?}", elapsed);
    println!("   (Notice: Source can produce faster than sink consumes)\n");

    // ============================================================================
    // PART 2: DISPATCHER (One Source → Multiple Sinks)
    // This is about routing items from ONE source to MULTIPLE sinks
    //
    // Mermaid Diagram:
    // ```mermaid
    // graph TD
    //     A["Source"] --> B["Pipeline"]
    //     B --> C["DispatcherSink"]
    //     C --> D["Dispatcher"]
    //
    //     D --> E["Sink 1<br/>(Logger)"]
    //     D --> F["Sink 2<br/>(Metrics)"]
    //     D --> G["Sink 3<br/>(Alerts)"]
    // ```
    // ============================================================================

    println!("=== 2. DISPATCHER (One Source → Multiple Sinks) ===");
    println!("This routes items from a single source to multiple sinks based on strategy.");
    println!("                    ┌─▶ Sink 1");
    println!("Source ─▶ Dispatcher├─▶ Sink 2");
    println!("                    └─▶ Sink 3\n");

    // Example 2a: Broadcast Dispatcher (Fan-out)
    println!("--- 2a. Broadcast Dispatcher (Every item goes to ALL sinks) ---");
    println!("Item 42 ─▶ ┌─▶ Logger: 42");
    println!("          ├─▶ Metrics: 42");
    println!("          └─▶ Alerts: 42");

    let broadcast_dispatcher = DispatchedPipelineBuilder::new().broadcast();

    // Subscribe multiple sinks - each will receive ALL items
    let logger_sink = sink_from_fn(|item: i64| async move {
        println!("  📝 Logger: Recording item {}", item);
        Ok(())
    });

    let metrics_sink = sink_from_fn(|item: i64| async move {
        println!("  📊 Metrics: Counting item {}", item);
        Ok(())
    });

    let _logger_id = broadcast_dispatcher
        .subscribe_sink(logger_sink, None)
        .await?;
    let _metrics_id = broadcast_dispatcher
        .subscribe_sink(metrics_sink, None)
        .await?;

    // Create a regular pipeline that feeds the dispatcher
    let source = RangeSource::new(1..4);
    let processor = NoOpProcessor::new();
    let pipeline = Pipeline::new(source, processor);

    // Use the dispatcher as a sink
    let dispatcher_sink = broadcast_dispatcher.create_sink(1);
    pipeline.sink(dispatcher_sink).await?;

    println!();

    // Example 2b: Demand Dispatcher (Load Balancing)
    println!("--- 2b. Demand Dispatcher (Items go to sink with most capacity) ---");
    println!("Items ─▶ ┌─▶ Fast Worker (high demand)");
    println!("         └─▶ Slow Worker (low demand)");

    let demand_dispatcher = DispatchedPipelineBuilder::new().demand();

    // Subscribe sinks with different processing speeds
    let fast_worker = sink_from_fn(|item: i64| async move {
        println!("  🏃 Fast worker: Processing item {} (10ms)", item);
        sleep(Duration::from_millis(10)).await;
        Ok(())
    });

    let slow_worker = sink_from_fn(|item: i64| async move {
        println!("  🐌 Slow worker: Processing item {} (50ms)", item);
        sleep(Duration::from_millis(50)).await;
        Ok(())
    });

    let _fast_id = demand_dispatcher.subscribe_sink(fast_worker, None).await?;
    let _slow_id = demand_dispatcher.subscribe_sink(slow_worker, None).await?;

    // Feed items through the demand dispatcher
    let source = RangeSource::new(1..8);
    let processor = NoOpProcessor::new();
    let pipeline = Pipeline::new(source, processor);

    let dispatcher_sink = demand_dispatcher.create_sink(1);
    pipeline.sink(dispatcher_sink).await?;

    println!();

    // Example 2c: Partition Dispatcher (Sharding)
    println!("--- 2c. Partition Dispatcher (Items routed by key/hash) ---");
    println!("Item 42 ─▶ hash(42) = even ─▶ Even Handler");
    println!("Item 43 ─▶ hash(43) = odd  ─▶ Odd Handler");

    let partitions = vec!["even".to_string(), "odd".to_string()];
    let partition_dispatcher =
        DispatchedPipelineBuilder::new().partition(partitions, |item: &i64| {
            if item % 2 == 0 {
                "even".to_string()
            } else {
                "odd".to_string()
            }
        });

    let even_sink = sink_from_fn(|item: i64| async move {
        println!("  🔢 Even handler: Processing {}", item);
        Ok(())
    });

    let odd_sink = sink_from_fn(|item: i64| async move {
        println!("  🎯 Odd handler: Processing {}", item);
        Ok(())
    });

    let _even_id = partition_dispatcher
        .subscribe_sink(even_sink, Some("even".to_string()))
        .await?;
    let _odd_id = partition_dispatcher
        .subscribe_sink(odd_sink, Some("odd".to_string()))
        .await?;

    let source = RangeSource::new(1..8);
    let processor = NoOpProcessor::new();
    let pipeline = Pipeline::new(source, processor);

    let dispatcher_sink = partition_dispatcher.create_sink(1);
    pipeline.sink(dispatcher_sink).await?;

    println!();

    // ============================================================================
    // SUMMARY
    // ============================================================================

    println!("=== SUMMARY ===");
    println!("🔄 CONCURRENT PIPELINE:");
    println!("   • Runs SOURCE and SINK in parallel");
    println!("   • Uses buffering between them");
    println!("   • Good for: I/O bound operations, preventing blocking");
    println!("   • Pattern: Source(Task1) ──[buffer]──> Sink(Task2)");
    println!();
    println!("📡 DISPATCHER:");
    println!("   • Routes items from ONE source to MULTIPLE sinks");
    println!("   • Different routing strategies (broadcast, demand, partition)");
    println!("   • Good for: Fan-out, load balancing, sharding");
    println!("   • Pattern: Source ──> Dispatcher ──> [Sink1, Sink2, Sink3]");
    println!();
    println!("💡 You can COMBINE them:");
    println!("   • Use ConcurrentPipeline for source↔sink parallelism");
    println!("   • Use Dispatcher for one-to-many routing");
    println!("   • Pattern: Source(Task1) ──[buffer]──> DispatcherSink(Task2) ──> [Sinks...]");

    Ok(())
}
