use std::io;

use metrics::SetRecorderError;
use thiserror::Error;

use crate::recorder::CsvRecorder;

/// Error type for the CSV exporter.
///
/// This is returned by recorder construction and write operations.
#[derive(Debug, Error)]
pub enum CsvError {
    /// I/O failures when creating, opening, or writing the CSV file.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Async CSV serialization failures.
    #[error(transparent)]
    CsvAsyncWriter(#[from] csv_async::Error),
}

/// Error type for builder and installation operations.
#[derive(Debug, Error)]
pub enum BuildError {
    /// Builder requires an output path via `CsvBuilder::with_path`.
    #[error("csv output path must be provided with CsvBuilder::with_path")]
    MissingPath,
    /// Failures when creating, opening, or writing the CSV file.
    #[error(transparent)]
    Csv(#[from] CsvError),
    /// Failed to install as the global recorder.
    #[error(transparent)]
    Install(#[from] SetRecorderError<CsvRecorder>),
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::CsvError;

    #[test]
    fn io_error_converts_to_csv_error() {
        let err = io::Error::other("boom");
        let csv_err: CsvError = err.into();
        assert!(matches!(csv_err, CsvError::Io(_)));
    }
}
