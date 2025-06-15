//! Error types for the producer/consumer system.

use std::fmt;
use std::sync::Arc;

/// The main error type for the producer/consumer system.
#[derive(Debug, Clone)]
pub enum Error {
    /// A producer failed to generate an item
    Producer(Arc<dyn std::error::Error + Send + Sync>),

    /// A consumer failed to process an item
    Consumer(Arc<dyn std::error::Error + Send + Sync>),

    /// A processor failed to transform an item
    Processor(Arc<dyn std::error::Error + Send + Sync>),

    /// The pipeline was shut down unexpectedly
    Shutdown,

    /// A channel was closed unexpectedly
    ChannelClosed,

    /// An operation timed out
    Timeout { duration_ms: u64 },

    /// The pipeline reached capacity and cannot accept more items
    Capacity { current: usize, max: usize },

    /// A custom error with a message
    Custom(String),

    /// Multiple errors occurred (e.g., in fan-out scenarios)
    Multiple(Vec<Error>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Producer(e) => write!(f, "Producer error: {}", e),
            Error::Consumer(e) => write!(f, "Consumer error: {}", e),
            Error::Processor(e) => write!(f, "Processor error: {}", e),
            Error::Shutdown => write!(f, "Pipeline was shut down"),
            Error::ChannelClosed => write!(f, "Channel was closed unexpectedly"),
            Error::Timeout { duration_ms } => {
                write!(f, "Operation timed out after {}ms", duration_ms)
            }
            Error::Capacity { current, max } => {
                write!(f, "Pipeline at capacity: {}/{}", current, max)
            }
            Error::Custom(msg) => write!(f, "{}", msg),
            Error::Multiple(errors) => {
                write!(f, "Multiple errors occurred: ")?;
                for (i, error) in errors.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "[{}]", error)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Producer(e) => Some(e.as_ref()),
            Error::Consumer(e) => Some(e.as_ref()),
            Error::Processor(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

// Convenience constructors
impl Error {
    /// Create a producer error from any error type
    pub fn producer<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Error::Producer(Arc::new(error))
    }

    /// Create a consumer error from any error type  
    pub fn consumer<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Error::Consumer(Arc::new(error))
    }

    /// Create a processor error from any error type
    pub fn processor<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Error::Processor(Arc::new(error))
    }

    /// Create a timeout error
    pub fn timeout(duration_ms: u64) -> Self {
        Error::Timeout { duration_ms }
    }

    /// Create a capacity error
    pub fn capacity(current: usize, max: usize) -> Self {
        Error::Capacity { current, max }
    }

    /// Create a custom error with a message
    pub fn custom<S: Into<String>>(message: S) -> Self {
        Error::Custom(message.into())
    }
}

// Common conversions
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::custom(err.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Error::ChannelClosed
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(e: tokio::time::error::Elapsed) -> Self {
        Error::Custom(format!("Timeout: {}", e))
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Error::Custom(e.to_string())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Custom(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Custom(s.to_string())
    }
}

/// Convenience type alias for Results in this crate
pub type Result<T> = std::result::Result<T, Error>;

/// Helper trait for converting errors into our Error type
pub trait IntoError<T> {
    fn into_producer_error(self) -> Result<T>;
    fn into_consumer_error(self) -> Result<T>;
    fn into_processor_error(self) -> Result<T>;
}

impl<T, E> IntoError<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_producer_error(self) -> Result<T> {
        self.map_err(Error::producer)
    }

    fn into_consumer_error(self) -> Result<T> {
        self.map_err(Error::consumer)
    }

    fn into_processor_error(self) -> Result<T> {
        self.map_err(Error::processor)
    }
}
