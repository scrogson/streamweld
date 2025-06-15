//! # GenStage-inspired Producer/Consumer System for Rust
//!
//! This crate provides a demand-driven data processing pipeline inspired by Elixir's GenStage,
//! but designed for Rust's ownership model and async ecosystem.
//!
//! ## Core Concepts
//!
//! - **Producer**: Generates data items on demand
//! - **Consumer**: Processes data items
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
//!     let producer = RangeProducer::new(1..101);
//!     let processor = MapProcessor::new(|x| x * 2);
//!     let consumer = PrintConsumer::<i64>::new();
//!
//!     Pipeline::new(producer, processor)
//!         .buffer_size(10)
//!         .sink(consumer)
//!         .await?;
//!     Ok(())
//! }
//! ```

pub mod dispatcher;
pub mod error;
pub mod impls;
pub mod pipeline;
pub mod traits;
pub mod util;

// Re-export commonly used items
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::impls::{consumers::*, processors::*, producers::*};
    pub use crate::pipeline::{ConcurrentPipeline, Pipeline, PipelineConfig, PipelineExt};
    pub use crate::traits::{Consumer, ConsumerExt, Processor, Producer, ProducerExt};
}

// Re-export main error type
pub use error::{Error, Result};

// Feature flags for optional dependencies
#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "tracing")]
pub mod tracing_support;

// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
