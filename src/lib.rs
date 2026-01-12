//! CSV exporter for the `metrics` crate.
//!
//! This crate provides a [`CsvRecorder`] that implements [`metrics::Recorder`],
//! plus a [`CsvBuilder`] for configuration and installation.
//! Output is long-form CSV with a fixed header: `timestamp_rfc3339,name,kind,labels,value`.
//! The `name` field is the sanitized metric name. The `labels` field is a `|`-delimited list of
//! `key=value` pairs in the order provided by the metrics key. Components are lowercased and
//! whitespace plus `|`/`=` characters are normalized to underscores.
//! It can be installed directly or composed with other recorders using `metrics-util` fanout
//! layers.
//!
//! Histograms are encoded in the `value` field as `value:count` pairs delimited by `|`
//! (values formatted to 6 decimals; non-finite values as `-inf`, `inf`, `NaN`).
//! Finite values are sorted numerically and non-finite values are appended; empty histograms
//! emit an empty `value` field.
//! Histogram buckets are cleared after each snapshot, so counts represent samples recorded since
//! the previous flush interval.
//!
//! The output file is created immediately when the recorder is constructed. The base output path
//! must not already exist; creation fails if it does. The CSV header is written once on creation.
//!
//! ## Output format
//! ```text
//! timestamp_rfc3339,name,kind,labels,value
//! 2026-01-08T12:00:00.123Z,requests_total,counter,method=get,42
//! 2026-01-08T12:00:00.123Z,temperature,gauge,,21.400000
//! 2026-01-08T12:00:00.123Z,latency_ms,histogram,,1.000000:2|2.000000:5|inf:1
//! ```
//!
//! ## Name and label encoding
//! ```text
//! http_requests_total
//! method=get|status=200
//! ```
//!
//! Labels are emitted in the order provided by the metrics key. Components are lowercased, and
//! whitespace plus `|`/`=` are normalized to underscores.
//!
//! ## Basic usage (Smol runtime)
//! ```no_run
//! use std::{time::Duration};
//! use metrics_exporter_csv::{CsvBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let (recorder, exporter) = CsvBuilder::default()
//!     .with_path("metrics.csv")
//!     .with_flush_interval(Duration::from_secs(3))
//!     .build()
//!     .expect("build recorder");
//!
//! metrics::set_global_recorder(recorder).expect("install recorder");
//! smol::spawn(exporter);
//! # Ok(())
//! # }
//! ```
//!
//! ## Deterministic shutdown
//! Drop the recorder to request a final flush. The exporter future should be allowed to run
//! until it completes.
//!
//! ## Runtime requirement
//! `CsvBuilder::build` returns an exporter future that must be driven by a Smol executor (for
//! example via `smol::spawn` or `smol::block_on`). `CsvBuilder::install` spawns a background
//! thread that runs a single-threaded Smol executor to drive the exporter future and returns
//! once the thread is started.
//!
//! When built with the `tokio` feature, `CsvBuilder::install_with_tokio` spawns the exporter
//! future onto the current Tokio runtime.

mod error;
mod recorder;
mod snapshot;
mod util;

pub use crate::{
    error::{BuildError, CsvError},
    recorder::{CsvBuilder, CsvRecorder, ExporterFuture},
};
