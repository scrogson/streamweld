//! Example implementations of simple sources, sinks, and processors.
//!
//! This module provides example implementations of simple sources, sinks, and processors
//! that can be used to build processing pipelines.

use streamweld::core::Result;
use streamweld::pipeline::Pipeline;
use streamweld::processors::MapProcessor;
use streamweld::sinks::PrintSink;
use streamweld::sources::RangeSource;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Simple Pipeline Example ===");

    let source = RangeSource::new(0..10);
    let processor = MapProcessor::new(|x: i64| x * 2);
    let sink = PrintSink::with_prefix("Processed: ".to_string());

    Pipeline::new(source, processor)
        .buffer_size(5)
        .sink(sink)
        .await?;

    println!("Simple pipeline completed!\n");
    Ok(())
}
