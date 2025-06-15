use std::time::Duration;
use streamweld::prelude::*;
use streamweld::utils::sink_from_fn;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    println!("StreamWeld Concurrent Pipeline Examples\n");

    // Example 1: Basic Concurrent Pipeline
    println!("=== Basic Concurrent Pipeline ===");
    let source = RangeSource::new(1..11);
    let sink = PrintSink::with_prefix("Concurrent".to_string());

    let pipeline = ConcurrentPipeline::new(source, sink)
        .buffer_size(5)
        .max_concurrency(2);

    pipeline.run().await?;
    println!("Basic concurrent pipeline completed!\n");

    // Example 2: Dispatcher with Multiple Sinks
    println!("=== Dispatcher with Multiple Sinks ===");
    let dispatched = DispatchedPipelineBuilder::new().buffer_size(10).broadcast();

    // Subscribe multiple sinks
    let sink1 = PrintSink::with_prefix("Sink-1".to_string());
    let sink2 = PrintSink::with_prefix("Sink-2".to_string());

    let _id1 = dispatched.subscribe_sink(sink1, None).await?;
    let _id2 = dispatched.subscribe_sink(sink2, None).await?;

    // Create a pipeline that feeds the dispatcher
    let source = RangeSource::new(1..6);
    let processor = NoOpProcessor::new();
    let pipeline = Pipeline::new(source, processor);

    let dispatcher_sink = dispatched.create_sink(1);
    pipeline.sink(dispatcher_sink).await?;
    println!("Dispatcher example completed!\n");

    // Example 3: Demand-based Dispatcher (Load Balancing)
    println!("=== Load Balancing with Demand Dispatcher ===");
    let dispatched = DispatchedPipelineBuilder::new().buffer_size(5).demand();

    // Create sinks with different processing speeds
    let fast_sink = sink_from_fn(|item: i64| async move {
        println!("🏃 Fast sink: {}", item);
        sleep(Duration::from_millis(10)).await;
        Ok(())
    });

    let slow_sink = sink_from_fn(|item: i64| async move {
        println!("🐌 Slow sink: {}", item);
        sleep(Duration::from_millis(50)).await;
        Ok(())
    });

    let _fast_id = dispatched.subscribe_sink(fast_sink, None).await?;
    let _slow_id = dispatched.subscribe_sink(slow_sink, None).await?;

    // Feed data through the dispatcher
    let source = RangeSource::new(1..11);
    let processor = NoOpProcessor::new();
    let pipeline = Pipeline::new(source, processor);

    let dispatcher_sink = dispatched.create_sink(1);
    pipeline.sink(dispatcher_sink).await?;
    println!("Load balancing example completed!\n");

    // Example 4: Partitioned Dispatcher
    println!("=== Partitioned Dispatcher ===");
    let partitions = vec!["even".to_string(), "odd".to_string()];
    let dispatched = DispatchedPipelineBuilder::new().partition(partitions, |item: &i64| {
        if item % 2 == 0 {
            "even".to_string()
        } else {
            "odd".to_string()
        }
    });

    // Subscribe sinks to specific partitions
    let even_sink = PrintSink::with_prefix("Even".to_string());
    let odd_sink = PrintSink::with_prefix("Odd".to_string());

    let _even_id = dispatched
        .subscribe_sink(even_sink, Some("even".to_string()))
        .await?;
    let _odd_id = dispatched
        .subscribe_sink(odd_sink, Some("odd".to_string()))
        .await?;

    // Feed data through the partitioned dispatcher
    let source = RangeSource::new(1..11);
    let processor = NoOpProcessor::new();
    let pipeline = Pipeline::new(source, processor);

    let dispatcher_sink = dispatched.create_sink(1);
    pipeline.sink(dispatcher_sink).await?;
    println!("Partitioned dispatcher example completed!\n");

    println!("All concurrent pipeline examples completed successfully!");
    Ok(())
}
