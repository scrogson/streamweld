use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use streamweld::pipeline::Pipeline;
use streamweld::prelude::*;
use streamweld::processors::{MapProcessor, NoOpProcessor};
use streamweld::sinks::CollectSink;
use streamweld::sources::RangeSource;

fn bench_basic_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_pipeline");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("noop", size), size, |b, &size| {
            b.iter(|| {
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let source = RangeSource::new(0..size);
                    let processor = NoOpProcessor::<i64>::new();
                    let sink = CollectSink::new();

                    Pipeline::new(source, processor).sink(sink).await.unwrap();
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("map", size), size, |b, &size| {
            b.iter(|| {
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let source = RangeSource::new(0..size);
                    let processor = MapProcessor::new(|x: i64| black_box(x * 2));
                    let sink = CollectSink::new();

                    Pipeline::new(source, processor).sink(sink).await.unwrap();
                })
            });
        });
    }

    group.finish();
}

fn bench_demand_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("demand_processing");

    for batch_size in [1, 10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("handle_demand", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    tokio::runtime::Runtime::new().unwrap().block_on(async {
                        let mut source = RangeSource::new(0..10000);
                        let mut total_items = 0;

                        while total_items < 10000 {
                            let items = source.handle_demand(black_box(batch_size)).await.unwrap();
                            if items.is_empty() {
                                break;
                            }
                            total_items += items.len();
                            black_box(items);
                        }
                    })
                });
            },
        );
    }

    group.finish();
}

fn bench_combinators(c: &mut Criterion) {
    let mut group = c.benchmark_group("combinators");

    group.bench_function("filter_map_take", |b| {
        b.iter(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let source = RangeSource::new(0..10000)
                    .filter(|x| x % 2 == 0)
                    .map(|x| black_box(x * 3))
                    .take(1000);

                let processor = NoOpProcessor::<i64>::new();
                let sink = CollectSink::new();

                Pipeline::new(source, processor).sink(sink).await.unwrap();
            })
        });
    });

    group.finish();
}

fn bench_batch_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_processing");

    for batch_size in [10, 50, 100, 500].iter() {
        group.bench_with_input(
            BenchmarkId::new("pipeline_batch_size", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    tokio::runtime::Runtime::new().unwrap().block_on(async {
                        let source = RangeSource::new(0..10000);
                        let processor = MapProcessor::new(|x: i64| black_box(x + 1));
                        let sink = CollectSink::new();

                        Pipeline::new(source, processor)
                            .demand_batch_size(batch_size)
                            .sink(sink)
                            .await
                            .unwrap();
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_basic_pipeline,
    bench_demand_processing,
    bench_combinators,
    bench_batch_processing
);
criterion_main!(benches);
