//! Dispatcher examples demonstrating different distribution patterns
//!
//! Run with: cargo run --example dispatcher_example

use std::time::Duration;
use streamweld::dispatcher::*;
use streamweld::prelude::*;
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

    // Create a dispatcher that distributes work to consumers with highest demand
    let dispatcher = DispatcherBuilder::new()
        .buffer_size(20)
        .max_demand(10)
        .demand();

    // Create multiple consumers with different processing speeds
    let fast_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🏃 Fast consumer processing: {}", item.data);
        sleep(Duration::from_millis(50)).await; // Fast processing
        Ok(())
    });

    let slow_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🐌 Slow consumer processing: {}", item.data);
        sleep(Duration::from_millis(200)).await; // Slow processing
        Ok(())
    });

    let medium_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🚶 Medium consumer processing: {}", item.data);
        sleep(Duration::from_millis(100)).await; // Medium processing
        Ok(())
    });

    // Subscribe consumers
    let consumer1_id = dispatcher.subscribe_consumer(fast_consumer, None).await?;
    let consumer2_id = dispatcher.subscribe_consumer(slow_consumer, None).await?;
    let consumer3_id = dispatcher.subscribe_consumer(medium_consumer, None).await?;

    // Generate work items
    let work_items: Vec<WorkItem> = (1..=20)
        .map(|i| WorkItem::new(i, "general", &format!("Task-{}", i)))
        .collect();

    // Dispatch work - should go mostly to fast consumer due to demand
    dispatcher.dispatch(work_items).await?;

    // Let consumers finish processing
    sleep(Duration::from_secs(2)).await;

    // Show consumer info
    let info = dispatcher.consumer_info().await;
    for consumer in info {
        println!(
            "Consumer {:?}: demand={}, partition={:?}",
            consumer.id, consumer.current_demand, consumer.partition
        );
    }

    println!();
    Ok(())
}

/// Example 2: Broadcast dispatcher (fan-out)
async fn broadcast_dispatcher_example() -> Result<()> {
    println!("=== Broadcast Dispatcher Example (Fan-out) ===");

    // Create a broadcast dispatcher that sends events to ALL consumers
    let dispatcher = DispatcherBuilder::new().buffer_size(10).broadcast();

    // Create multiple consumers for different purposes
    let logger_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("📝 Logger: Recorded {}", item.data);
        Ok(())
    });

    let metrics_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("📊 Metrics: Counting item in category '{}'", item.category);
        Ok(())
    });

    let backup_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("💾 Backup: Archiving item {}", item.id);
        Ok(())
    });

    // Subscribe all consumers
    dispatcher.subscribe_consumer(logger_consumer, None).await?;
    dispatcher
        .subscribe_consumer(metrics_consumer, None)
        .await?;
    dispatcher.subscribe_consumer(backup_consumer, None).await?;

    // Generate events
    let events = vec![
        WorkItem::new(1, "important", "Critical data"),
        WorkItem::new(2, "normal", "Regular data"),
        WorkItem::new(3, "urgent", "Time-sensitive data"),
    ];

    // Broadcast to all consumers
    dispatcher.dispatch(events).await?;

    sleep(Duration::from_millis(500)).await;
    println!();
    Ok(())
}

/// Example 3: Broadcast with selector (filtered fan-out)
async fn broadcast_with_selector_example() -> Result<()> {
    println!("=== Broadcast with Selector Example (Filtered Fan-out) ===");

    // Create consumers with selectors for different categories
    let important_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🚨 Important handler: {}", item.data);
        Ok(())
    });

    let normal_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("📋 Normal handler: {}", item.data);
        Ok(())
    });

    // Create separate dispatchers with different selectors
    // Note: In a real implementation, you'd want a single dispatcher with per-consumer selectors
    let important_dispatcher = DispatcherBuilder::new()
        .broadcast_with_selector(|item: &WorkItem| item.category == "important");

    let normal_dispatcher = DispatcherBuilder::new()
        .broadcast_with_selector(|item: &WorkItem| item.category == "normal");

    important_dispatcher
        .subscribe_consumer(important_consumer, None)
        .await?;
    normal_dispatcher
        .subscribe_consumer(normal_consumer, None)
        .await?;

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

    // Create consumers for each partition
    let consumer_a = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🅰️  Partition A consumer: {}", item.data);
        Ok(())
    });

    let consumer_b = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🅱️  Partition B consumer: {}", item.data);
        Ok(())
    });

    let consumer_c = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🅲  Partition C consumer: {}", item.data);
        Ok(())
    });

    // Subscribe consumers to specific partitions
    dispatcher
        .subscribe_consumer(consumer_a, Some("A".to_string()))
        .await?;
    dispatcher
        .subscribe_consumer(consumer_b, Some("B".to_string()))
        .await?;
    dispatcher
        .subscribe_consumer(consumer_c, Some("C".to_string()))
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
        high_priority_consumers: std::sync::Mutex<Vec<ConsumerId>>,
        low_priority_consumers: std::sync::Mutex<Vec<ConsumerId>>,
    }

    impl PriorityDispatcher {
        fn new() -> Self {
            Self {
                high_priority_consumers: std::sync::Mutex::new(Vec::new()),
                low_priority_consumers: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl CustomDispatcher<WorkItem> for PriorityDispatcher {
        fn subscribe(&self, consumer_id: ConsumerId, partition: Option<String>) -> Result<()> {
            match partition.as_deref() {
                Some("high") => {
                    self.high_priority_consumers
                        .lock()
                        .unwrap()
                        .push(consumer_id);
                }
                Some("low") | None => {
                    self.low_priority_consumers
                        .lock()
                        .unwrap()
                        .push(consumer_id);
                }
                _ => return Err(Error::custom("Invalid partition")),
            }
            Ok(())
        }

        fn unsubscribe(&self, consumer_id: ConsumerId) -> Result<()> {
            self.high_priority_consumers
                .lock()
                .unwrap()
                .retain(|&id| id != consumer_id);
            self.low_priority_consumers
                .lock()
                .unwrap()
                .retain(|&id| id != consumer_id);
            Ok(())
        }

        fn dispatch(
            &self,
            events: Vec<WorkItem>,
            consumers: &std::collections::HashMap<ConsumerId, ConsumerHandle<WorkItem>>,
        ) -> Result<()> {
            for event in events {
                let target_consumers = if event.category == "urgent" {
                    &self.high_priority_consumers
                } else {
                    &self.low_priority_consumers
                };

                // Route to appropriate consumer pool
                let consumer_ids = target_consumers.lock().unwrap();
                if !consumer_ids.is_empty() {
                    let target_id = consumer_ids[event.id as usize % consumer_ids.len()];
                    if let Some(_handle) = consumers.get(&target_id) {
                        // In real implementation, send event to handle.sender
                        println!(
                            "Custom dispatcher: Routing {} to consumer {:?}",
                            event.data, target_id
                        );
                    }
                }
            }
            Ok(())
        }

        fn handle_demand(&self, _consumer_id: ConsumerId, _demand: usize) -> Result<()> {
            // Custom demand handling logic
            Ok(())
        }
    }

    let custom_dispatcher = PriorityDispatcher::new();
    let dispatcher = DispatcherBuilder::new().custom(custom_dispatcher);

    // Subscribe consumers to different priority levels
    let high_priority_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("🔥 High priority consumer: {}", item.data);
        Ok(())
    });

    let low_priority_consumer = streamweld::util::consumer_from_fn(|item: WorkItem| async move {
        println!("📝 Low priority consumer: {}", item.data);
        Ok(())
    });

    dispatcher
        .subscribe_consumer(high_priority_consumer, Some("high".to_string()))
        .await?;
    dispatcher
        .subscribe_consumer(low_priority_consumer, Some("low".to_string()))
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

    let producer = RangeProducer::new(1..11);
    let processor = MapProcessor::new(|x| WorkItem::new(x as u64, "data", &format!("Item-{}", x)));

    // Collect processed items
    let consumer = CollectConsumer::new();

    Pipeline::new(producer, processor)
        .sink(consumer.clone())
        .await?;

    // Get the results and dispatch them
    let items = consumer.items();
    let collected = items.lock().await.clone();

    // Now use a dispatcher to distribute the collected items
    let dispatcher = DispatcherBuilder::new().demand();

    let consumer1 = PrintConsumer::with_prefix("Consumer-1".to_string());
    let consumer2 = PrintConsumer::with_prefix("Consumer-2".to_string());

    dispatcher.subscribe_consumer(consumer1, None).await?;
    dispatcher.subscribe_consumer(consumer2, None).await?;

    dispatcher.dispatch(collected).await?;

    sleep(Duration::from_millis(500)).await;
    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("GenStage-Rust Dispatcher Examples\n");

    demand_dispatcher_example().await?;
    broadcast_dispatcher_example().await?;
    broadcast_with_selector_example().await?;
    partition_dispatcher_example().await?;
    custom_dispatcher_example().await?;
    pipeline_with_dispatcher_example().await?;

    println!("All dispatcher examples completed!");
    Ok(())
}
