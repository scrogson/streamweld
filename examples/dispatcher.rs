//! Dispatcher examples demonstrating different distribution patterns
//!
//! Run with: cargo run --example dispatcher_example

use std::sync::Arc;
use std::time::Duration;
use streamweld::prelude::*;
use streamweld::processors::MapProcessor;
use streamweld::sinks::PrintSink;
use streamweld::sources::RangeSource;
use streamweld::utils::dispatcher::*;
use tokio::time::sleep;

/// Example data type for demonstrations
#[derive(Debug, Clone)]
struct WorkItem {
    id: u64,
    category: String,
    data: String,
}

impl std::fmt::Display for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkItem(id={}, category={}, data={})",
            self.id, self.category, self.data
        )
    }
}

impl WorkItem {
    fn new(id: u64, category: &str, data: &str) -> Self {
        Self {
            id,
            category: category.to_string(),
            data: data.to_string(),
        }
    }
}

/// Example 1: Demand-based dispatcher (load balancing)
async fn demand_dispatcher_example() -> Result<()> {
    println!("=== Demand Dispatcher Example (Load Balancing) ===");

    // Create a dispatcher that distributes work to sinks with highest demand
    let dispatcher = DispatcherBuilder::new()
        .buffer_size(20)
        .max_demand(10)
        .demand();

    // Create multiple sinks with different processing speeds
    let fast_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🏃 Fast sink processing: {}", item.data);
        sleep(Duration::from_millis(50)).await; // Fast processing
        Ok(())
    });

    let slow_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🐌 Slow sink processing: {}", item.data);
        sleep(Duration::from_millis(200)).await; // Slow processing
        Ok(())
    });

    let medium_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🚶 Medium sink processing: {}", item.data);
        sleep(Duration::from_millis(100)).await; // Medium processing
        Ok(())
    });

    // Subscribe sinks
    let _sink1_id = dispatcher.subscribe_sink(fast_sink, None).await?;
    let _sink2_id = dispatcher.subscribe_sink(slow_sink, None).await?;
    let _sink3_id = dispatcher.subscribe_sink(medium_sink, None).await?;

    // Generate work items
    let work_items: Vec<WorkItem> = (1..=20)
        .map(|i| WorkItem::new(i, "general", &format!("Task-{}", i)))
        .collect();

    // Dispatch work - should go mostly to fast sink due to demand
    dispatcher.dispatch(work_items).await?;

    // Let sinks finish processing
    sleep(Duration::from_secs(2)).await;

    // Show sink info
    let info = dispatcher.sink_info().await;
    for sink in info {
        println!(
            "Sink {:?}: demand={}, partition={:?}",
            sink.id, sink.current_demand, sink.partition
        );
    }

    println!();
    Ok(())
}

/// Example 2: Broadcast dispatcher (fan-out)
async fn broadcast_dispatcher_example() -> Result<()> {
    println!("=== Broadcast Dispatcher Example (Fan-out) ===");

    // Create a broadcast dispatcher that sends events to ALL sinks
    let dispatcher = DispatcherBuilder::new().buffer_size(10).broadcast();

    // Create multiple sinks for different purposes
    let logger_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("📝 Logger: Recorded {}", item.data);
        Ok(())
    });

    let metrics_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("📊 Metrics: Counting item in category '{}'", item.category);
        Ok(())
    });

    let backup_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("💾 Backup: Archiving item {}", item.id);
        Ok(())
    });

    // Subscribe all sinks
    dispatcher.subscribe_sink(logger_sink, None).await?;
    dispatcher.subscribe_sink(metrics_sink, None).await?;
    dispatcher.subscribe_sink(backup_sink, None).await?;

    // Generate events
    let events = vec![
        WorkItem::new(1, "important", "Critical data"),
        WorkItem::new(2, "normal", "Regular data"),
        WorkItem::new(3, "urgent", "Time-sensitive data"),
    ];

    // Broadcast to all sinks
    dispatcher.dispatch(events).await?;

    sleep(Duration::from_millis(500)).await;
    println!();
    Ok(())
}

/// Example 3: Broadcast with selector (filtered fan-out)
async fn broadcast_with_selector_example() -> Result<()> {
    println!("=== Broadcast with Selector Example (Filtered Fan-out) ===");

    // Create sinks with selectors for different categories
    let important_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🚨 Important handler: {}", item.data);
        Ok(())
    });

    let normal_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("📋 Normal handler: {}", item.data);
        Ok(())
    });

    // Create separate dispatchers with different selectors
    // Note: In a real implementation, you'd want a single dispatcher with per-sink selectors
    let important_dispatcher = DispatcherBuilder::new()
        .broadcast_with_selector(|item: &WorkItem| item.category == "important");

    let normal_dispatcher = DispatcherBuilder::new()
        .broadcast_with_selector(|item: &WorkItem| item.category == "normal");

    important_dispatcher
        .subscribe_sink(important_sink, None)
        .await?;
    normal_dispatcher.subscribe_sink(normal_sink, None).await?;

    // Generate mixed events
    let events = vec![
        WorkItem::new(1, "important", "Critical alert"),
        WorkItem::new(2, "normal", "Regular update"),
        WorkItem::new(3, "important", "Security incident"),
        WorkItem::new(4, "normal", "Status report"),
    ];

    // Dispatch to both - each will filter appropriately
    for event in &events {
        important_dispatcher.dispatch(vec![event.clone()]).await?;
        normal_dispatcher.dispatch(vec![event.clone()]).await?;
    }

    sleep(Duration::from_millis(300)).await;
    println!();
    Ok(())
}

/// Example 4: Partition dispatcher (sharding)
async fn partition_dispatcher_example() -> Result<()> {
    println!("=== Partition Dispatcher Example (Sharding) ===");

    // Create a partition dispatcher that routes by category
    let partitions = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let dispatcher = DispatcherBuilder::new().partition(partitions, |item: &WorkItem| {
        // Hash by first character of category
        item.category
            .chars()
            .next()
            .unwrap_or('A')
            .to_string()
            .to_uppercase()
    });

    // Create sinks for each partition
    let sink_a = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🅰️  Partition A sink: {}", item.data);
        Ok(())
    });

    let sink_b = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🅱️  Partition B sink: {}", item.data);
        Ok(())
    });

    let sink_c = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🅲  Partition C sink: {}", item.data);
        Ok(())
    });

    // Subscribe sinks to specific partitions
    dispatcher
        .subscribe_sink(sink_a, Some("A".to_string()))
        .await?;
    dispatcher
        .subscribe_sink(sink_b, Some("B".to_string()))
        .await?;
    dispatcher
        .subscribe_sink(sink_c, Some("C".to_string()))
        .await?;

    // Generate events with different categories
    let events = vec![
        WorkItem::new(1, "apple", "Apple data"),     // -> Partition A
        WorkItem::new(2, "banana", "Banana data"),   // -> Partition B
        WorkItem::new(3, "cherry", "Cherry data"),   // -> Partition C
        WorkItem::new(4, "apricot", "Apricot data"), // -> Partition A
        WorkItem::new(5, "blueberry", "Berry data"), // -> Partition B
        WorkItem::new(6, "coconut", "Coconut data"), // -> Partition C
    ];

    // Dispatch - should route to appropriate partitions
    dispatcher.dispatch(events).await?;

    sleep(Duration::from_millis(500)).await;
    println!();
    Ok(())
}

/// Example 5: Custom dispatcher (custom routing logic)
async fn custom_dispatcher_example() -> Result<()> {
    println!("=== Custom Dispatcher Example ===");

    // Define a custom dispatcher that routes based on priority
    struct PriorityDispatcher {
        high_priority_sinks: std::sync::Mutex<Vec<SinkId>>,
        low_priority_sinks: std::sync::Mutex<Vec<SinkId>>,
    }

    impl PriorityDispatcher {
        fn new() -> Self {
            Self {
                high_priority_sinks: std::sync::Mutex::new(Vec::new()),
                low_priority_sinks: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl CustomDispatcher<WorkItem> for PriorityDispatcher {
        fn subscribe(&self, sink_id: SinkId, partition: Option<String>) -> Result<()> {
            match partition.as_deref() {
                Some("high") => {
                    self.high_priority_sinks.lock().unwrap().push(sink_id);
                }
                Some("low") | None => {
                    self.low_priority_sinks.lock().unwrap().push(sink_id);
                }
                _ => return Err(Error::custom("Invalid partition")),
            }
            Ok(())
        }

        fn unsubscribe(&self, sink_id: SinkId) -> Result<()> {
            self.high_priority_sinks
                .lock()
                .unwrap()
                .retain(|&id| id != sink_id);
            self.low_priority_sinks
                .lock()
                .unwrap()
                .retain(|&id| id != sink_id);
            Ok(())
        }

        fn dispatch(
            &self,
            events: Vec<WorkItem>,
            sinks: &std::collections::HashMap<SinkId, SinkHandle<WorkItem>>,
        ) -> Result<()> {
            for event in events {
                let target_sinks = if event.category == "urgent" {
                    &self.high_priority_sinks
                } else {
                    &self.low_priority_sinks
                };

                // Route to appropriate sink pool
                let sink_ids = target_sinks.lock().unwrap();
                if !sink_ids.is_empty() {
                    let idx = event.id.try_into().unwrap_or(0) % sink_ids.len();
                    let target_id = sink_ids[idx];
                    if let Some(_handle) = sinks.get(&target_id) {
                        // In real implementation, send event to handle.sender
                        println!(
                            "Custom dispatcher: Routing {} to sink {:?}",
                            event.data, target_id
                        );
                    }
                }
            }
            Ok(())
        }

        fn handle_demand(&self, _sink_id: SinkId, _demand: usize) -> Result<()> {
            // Custom demand handling logic
            Ok(())
        }
    }

    let custom_dispatcher = PriorityDispatcher::new();
    let dispatcher = DispatcherBuilder::new().custom(custom_dispatcher);

    // Subscribe sinks to different priority levels
    let high_priority_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("🔥 High priority sink: {}", item.data);
        Ok(())
    });

    let low_priority_sink = streamweld::utils::sink_from_fn(|item: WorkItem| async move {
        println!("📝 Low priority sink: {}", item.data);
        Ok(())
    });

    dispatcher
        .subscribe_sink(high_priority_sink, Some("high".to_string()))
        .await?;
    dispatcher
        .subscribe_sink(low_priority_sink, Some("low".to_string()))
        .await?;

    // Generate events with different priorities
    let events = vec![
        WorkItem::new(1, "urgent", "Critical system alert"),
        WorkItem::new(2, "normal", "Regular log entry"),
        WorkItem::new(3, "urgent", "Security breach"),
        WorkItem::new(4, "normal", "Status update"),
    ];

    dispatcher.dispatch(events).await?;

    sleep(Duration::from_millis(300)).await;
    println!();
    Ok(())
}

/// Example 6: Pipeline with dispatcher integration
async fn pipeline_with_dispatcher_example() -> Result<()> {
    println!("=== Pipeline with Dispatcher Integration ===");

    // This would be the ideal API - a pipeline that outputs to a dispatcher
    // For now, this is conceptual since we'd need to integrate dispatchers into pipelines

    let source = RangeSource::new(1..11);
    let processor: MapProcessor<_, i64, WorkItem> =
        MapProcessor::new(|x: i64| WorkItem::new(x as u64, "data", &format!("Item-{}", x)));

    // Collect processed items
    let collector: CollectSink<WorkItem> = CollectSink::new();

    Pipeline::new(source, processor)
        .sink(collector.clone())
        .await?;

    // Get the results and dispatch them
    let items = collector.items();
    let collected: Vec<WorkItem> = items.lock().await.iter().cloned().collect();

    // Now use a dispatcher to distribute the collected items
    let dispatcher = Arc::new(DispatcherBuilder::<WorkItem>::new().demand());

    let sink1 = PrintSink::with_prefix("Sink-1".to_string());
    let sink2 = PrintSink::with_prefix("Sink-2".to_string());

    dispatcher.subscribe_sink(sink1, None).await?;
    dispatcher.subscribe_sink(sink2, None).await?;

    dispatcher.dispatch(collected).await?;

    sleep(Duration::from_millis(500)).await;
    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("StreamWeld Dispatcher Examples\n");

    demand_dispatcher_example().await?;
    broadcast_dispatcher_example().await?;
    broadcast_with_selector_example().await?;
    partition_dispatcher_example().await?;
    custom_dispatcher_example().await?;
    pipeline_with_dispatcher_example().await?;

    println!("All dispatcher examples completed!");
    Ok(())
}
