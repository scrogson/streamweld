//! # GenStage-inspired Source/Sink System for Rust
//!
//! This crate provides a demand-driven data processing pipeline inspired by Elixir's GenStage,
//! but designed for Rust's ownership model and async ecosystem.
//!
//! ## Core Concepts
//!
//! - **Source**: Generates data items on demand
//! - **Sink**: Processes data items
//! - **Processor**: Transforms data (reads from upstream, processes, sends downstream)
//! - **Pipeline**: Connects stages with backpressure control
//!
//! ## Example
//!
//! ```rust
//! use streamweld::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let source = RangeSource::new(1..101);
//!     let processor = MapProcessor::new(|x| x * 2);
//!     let sink = PrintSink::<i64>::new();
//!
//!     Pipeline::new(source, processor)
//!         .buffer_size(10)
//!         .sink(sink)
//!         .await?;
//!     Ok(())
//! }
//! ```

pub mod core;
pub mod pipeline;
pub mod processors;
pub mod sinks;
pub mod sources;
pub mod utils;

// Re-export commonly used items
pub mod prelude {
    pub use crate::core::{Error, Processor, Result, Sink, Source, SourceExt};
    pub use crate::pipeline::{
        dispatcher::{DispatchedPipeline, DispatchedPipelineBuilder, DispatcherSink},
        ConcurrentPipeline, Pipeline, PipelineConfig, PipelineExt,
    };
    pub use crate::processors::*;
    pub use crate::sinks::*;
    pub use crate::sources::*;
}

// Re-export main error type
pub use core::{Error, Result};

// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
