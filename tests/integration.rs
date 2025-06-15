//! Integration tests for the GenStage-inspired producer/consumer system

use std::time::Duration;
use streamweld::prelude::*;
use streamweld::traits::ProducerExt;

#[tokio::test]
async fn test_basic_pipeline() {
    let producer = RangeProducer::new(1..6);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, NoOpProcessor::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_pipeline_with_processing() {
    let producer = RangeProducer::new(1..6);
    let processor = MapProcessor::new(|x| x * 2);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, processor)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_filter_processor() {
    let producer = RangeProducer::new(1..11);
    let filter = FilterProcessor::new(|x: &i64| x % 2 == 0);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, filter)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_batch_processor() {
    let producer = RangeProducer::new(1..8);
    let batcher = BatchProcessor::new(3);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, batcher)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(collected.len(), 3); // 3 batches: [1,2,3], [4,5,6], [7]
    assert_eq!(collected[0], vec![1, 2, 3]);
    assert_eq!(collected[1], vec![4, 5, 6]);
    assert_eq!(collected[2], vec![7]);
}

#[tokio::test]
async fn test_complex_pipeline() {
    let producer = RangeProducer::new(1..21)
        .filter(|x| x % 2 == 0)
        .map(|x| x * 3)
        .take(3);
    let processor = NoOpProcessor::<i64>::new();
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, processor)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![6, 12, 18]); // 2*3, 4*3, 6*3
}

#[tokio::test]
async fn test_combinators() {
    let producer = RangeProducer::new(1..11);
    let processor = NoOpProcessor::<i64>::new();
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    let combined = producer
        .filter(|x| x % 2 == 0) // Even numbers
        .map(|x| x * 2); // Double them

    Pipeline::new(combined, processor)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![4, 8, 12, 16, 20]); // 2*2, 4*2, 6*2, 8*2, 10*2
}

#[tokio::test]
async fn test_concurrent_pipeline() {
    let producer = RangeProducer::new(1..101);
    let processor = NoOpProcessor::<i64>::new();
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    let pipeline = ConcurrentPipeline::new(producer, collector)
        .buffer_size(10)
        .max_concurrency(4);

    pipeline.run().await.unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(collected.len(), 100);

    // Items might not be in order due to concurrency, so sort for comparison
    let mut sorted = collected.clone();
    sorted.sort();
    assert_eq!(sorted, (1..101).collect::<Vec<_>>());
}

#[tokio::test]
async fn test_error_handling() {
    // Create a producer that produces all items, including a special error value
    let producer = streamweld::util::from_fn(|| async {
        static mut COUNTER: i32 = 0;
        unsafe {
            COUNTER += 1;
            if COUNTER > 5 {
                return Ok(None);
            }
            // Instead of returning an error, produce a special value
            if COUNTER == 3 {
                return Ok(Some(-1)); // Special value indicating error condition
            }
            Ok(Some(COUNTER))
        }
    });

    // Create a processor that handles the special error value
    let error_handler = MapProcessor::new(|x: i32| {
        if x == -1 {
            Err(Error::custom("Test error"))
        } else {
            Ok(x * 2)
        }
    });
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, error_handler)
        .fail_fast(false)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;

    // Should have 5 results: 4 successful transformations and 1 error
    assert_eq!(collected.len(), 5);

    // Check successful items
    assert!(collected[0].is_ok());
    assert_eq!(collected[0].as_ref().unwrap(), &2); // 1 * 2
    assert!(collected[1].is_ok());
    assert_eq!(collected[1].as_ref().unwrap(), &4); // 2 * 2

    // Check error item
    assert!(collected[2].is_err());

    // Check remaining successful items
    assert!(collected[3].is_ok());
    assert_eq!(collected[3].as_ref().unwrap(), &8); // 4 * 2
    assert!(collected[4].is_ok());
    assert_eq!(collected[4].as_ref().unwrap(), &10); // 5 * 2
}

#[tokio::test]
async fn test_pipeline_timeout() {
    let producer = streamweld::util::from_fn(|| async {
        // Simulate slow production
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Some(42))
    });

    let processor = NoOpProcessor::<i32>::new();
    let collector = CollectConsumer::new();

    let pipeline = Pipeline::new(producer, processor).operation_timeout(Duration::from_millis(50)); // Shorter than production time

    let result = pipeline.sink(collector).await;
    assert!(matches!(result.unwrap_err(), Error::Timeout { .. }));
}

#[tokio::test]
async fn test_fibonacci_producer() {
    let producer = FibonacciProducer::with_limit(8);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, NoOpProcessor::<u64>::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![0, 1, 1, 2, 3, 5, 8, 13]);
}

#[tokio::test]
async fn test_repeat_producer() {
    let producer = RepeatProducer::times("hello", 3);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, NoOpProcessor::<&str>::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec!["hello", "hello", "hello"]);
}

#[tokio::test]
async fn test_count_consumer() {
    let producer = RangeProducer::new(1..11);
    let counter = CountConsumer::new();
    let counter_ref = counter.clone();

    Pipeline::new(producer, NoOpProcessor::<i64>::new())
        .sink(counter)
        .await
        .unwrap();

    assert_eq!(counter_ref.count().await, 10);
}

#[tokio::test]
async fn test_vec_producer() {
    let items = vec!["a", "b", "c", "d"];
    let producer = VecProducer::new(items.clone());
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, NoOpProcessor::<&str>::new())
        .sink(collector)
        .await
        .unwrap();

    let items_arc = collector_ref.items();
    let collected = items_arc.lock().await;
    assert_eq!(*collected, items);
}

#[tokio::test]
async fn test_chain_producer() {
    let producer1 = RangeProducer::new(1..4);
    let producer2 = RangeProducer::new(4..7);
    let chained = producer1.chain(producer2);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(chained, NoOpProcessor::<i64>::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_empty_producer() {
    let producer = VecProducer::<i32>::new(vec![]);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    Pipeline::new(producer, NoOpProcessor::<i32>::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert!(collected.is_empty());
}

#[tokio::test]
async fn test_pipeline_shutdown() {
    let producer = RangeProducer::new(1..6);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    let pipeline = Pipeline::new(producer, NoOpProcessor::<i64>::new()).buffer_size(5);
    pipeline.sink(collector).await.unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![1, 2, 3, 4, 5]);
}
