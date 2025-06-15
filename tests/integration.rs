//! Integration tests for the GenStage-inspired producer/consumer system

use std::time::Duration;
use streamweld::core::SourceExt;
use streamweld::prelude::*;

#[tokio::test]
async fn test_basic_pipeline() -> Result<()> {
    let source = RangeSource::new(0..10);
    let _processor = NoOpProcessor::<i64>::new();
    let sink = PrintSink::with_prefix("Test: ".to_string());

    let _pipeline = Pipeline::new(source, _processor)
        .buffer_size(5)
        .sink(sink)
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_pipeline_with_processing() {
    let source = RangeSource::new(1..6);
    let processor = MapProcessor::new(|x| x * 2);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, processor)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_filter_processor() {
    let source = RangeSource::new(1..11);
    let filter = FilterProcessor::new(|x: &i64| x % 2 == 0);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, filter).sink(collector).await.unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_batch_processor() {
    let source = RangeSource::new(1..8);
    let batcher = BatchProcessor::new(3);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, batcher)
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
    let source = RangeSource::new(1..21)
        .filter(|x| x % 2 == 0)
        .map(|x| x * 3)
        .take(3);
    let _processor = NoOpProcessor::<i64>::new();
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, _processor)
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![6, 12, 18]); // 2*3, 4*3, 6*3
}

#[tokio::test]
async fn test_combinators() {
    let source = RangeSource::new(1..11);
    let processor = NoOpProcessor::<i64>::new();
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    let combined = source
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
    let source = RangeSource::new(1..101);
    let sink: CollectSink<i64> = CollectSink::new();
    let sink_ref = sink.clone();

    let pipeline = ConcurrentPipeline::new(source, sink)
        .buffer_size(10)
        .max_concurrency(4);

    pipeline.run().await.unwrap();

    let items = sink_ref.items();
    let collected = items.lock().await;
    assert_eq!(collected.len(), 100);

    // Items might not be in order due to concurrency, so sort for comparison
    let mut sorted = collected.clone();
    sorted.sort();
    assert_eq!(sorted, (1..101).collect::<Vec<_>>());
}

#[tokio::test]
async fn test_error_handling() {
    // Create a source that produces all items, including a special error value
    let source = streamweld::utils::from_fn(|| async {
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
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, error_handler)
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
    let source = streamweld::utils::from_fn(|| async {
        // Simulate slow production
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Some(42))
    });

    let processor = NoOpProcessor::<i32>::new();
    let collector = CollectSink::new();

    let pipeline = Pipeline::new(source, processor).operation_timeout(Duration::from_millis(50)); // Shorter than production time

    let result = pipeline.sink(collector).await;
    assert!(matches!(result.unwrap_err(), Error::Timeout { .. }));
}

#[tokio::test]
async fn test_fibonacci_source() {
    let source = FibonacciSource::with_limit(8);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, NoOpProcessor::<u64>::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![0, 1, 1, 2, 3, 5, 8, 13]);
}

#[tokio::test]
async fn test_repeat_source() {
    let source = RepeatSource::times("hello", 3);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, NoOpProcessor::<&str>::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec!["hello", "hello", "hello"]);
}

#[tokio::test]
async fn test_count_sink() {
    let source = RangeSource::new(1..11);
    let counter = CountSink::new();
    let counter_ref = counter.clone();

    Pipeline::new(source, NoOpProcessor::<i64>::new())
        .sink(counter)
        .await
        .unwrap();

    assert_eq!(counter_ref.count().await, 10);
}

#[tokio::test]
async fn test_vec_source() {
    let items = vec!["a", "b", "c", "d"];
    let source = VecSource::new(items.clone());
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, NoOpProcessor::<&str>::new())
        .sink(collector)
        .await
        .unwrap();

    let items_arc = collector_ref.items();
    let collected = items_arc.lock().await;
    assert_eq!(*collected, items);
}

#[tokio::test]
async fn test_chain_source() {
    let source1 = RangeSource::new(1..4);
    let source2 = RangeSource::new(4..7);
    let chained = source1.chain(source2);
    let collector = CollectSink::new();
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
async fn test_empty_source() {
    let source = VecSource::<i32>::new(vec![]);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    Pipeline::new(source, NoOpProcessor::<i32>::new())
        .sink(collector)
        .await
        .unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert!(collected.is_empty());
}

#[tokio::test]
async fn test_pipeline_shutdown() {
    let source = RangeSource::new(1..6);
    let collector = CollectSink::new();
    let collector_ref = collector.clone();

    let pipeline = Pipeline::new(source, NoOpProcessor::<i64>::new()).buffer_size(5);
    pipeline.sink(collector).await.unwrap();

    let items = collector_ref.items();
    let collected = items.lock().await;
    assert_eq!(*collected, vec![1, 2, 3, 4, 5]);
}
