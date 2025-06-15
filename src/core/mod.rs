//! Core traits and types for the streamweld library.
//!
//! This module contains the fundamental traits and error types that define
//! the streamweld processing model.

pub mod error;
pub mod traits;

// Re-export core items
pub use error::{Error, Result};
pub use traits::{Processor, Sink, Source, SourceExt};
