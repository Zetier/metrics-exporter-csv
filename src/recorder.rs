#[cfg(feature = "prometheus")]
use crate::SnapshotSource;

use std::{
    fs::{self, OpenOptions},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    thread,
    time::Duration,
};

use csv_async::AsyncWriter;
use event_listener::{Event, EventListener};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};
use smol::{Timer, fs::File, prelude::*, stream};
#[cfg(feature = "tokio")]
use tokio::task;
use tracing::error;

use crate::{
    error::{BuildError, CsvError},
    snapshot::write_snapshot,
};

pub const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 3;
const HEADER: [&str; 5] = ["timestamp_rfc3339", "name", "kind", "labels", "value"];

/// CSV recorder implementing [`metrics::Recorder`].
///
/// Background upkeep writes periodic snapshots of all known metrics when built via
/// [`CsvBuilder`](crate::CsvBuilder). Each snapshot writes one row per metric in a
/// long-form CSV file. The `name` field is the sanitized metric name and `labels` contains
/// `key=value` pairs delimited by `|`, emitted in the order provided by the metrics key, with
/// lowercase normalization and whitespace/`|`/`=` replaced by underscores.
///
/// The output file is created immediately when the recorder is constructed. If the base path
/// already exists, construction fails with an error. The CSV header is written once on creation.
pub struct CsvRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
    notify: Event,
}

impl CsvRecorder {
    fn new(registry: Arc<Registry<Key, AtomicStorage>>, notify: Event) -> Self {
        Self { registry, notify }
    }
}

impl Drop for CsvRecorder {
    fn drop(&mut self) {
        self.notify.notify_relaxed(1);
    }
}

impl Recorder for CsvRecorder {
    // Description metadata is accepted but not used by CsvRecorder.
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let handle = self.registry.get_or_create_counter(key, Arc::clone);
        Counter::from_arc(handle)
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        let handle = self.registry.get_or_create_gauge(key, Arc::clone);
        Gauge::from_arc(handle)
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        let handle = self.registry.get_or_create_histogram(key, Arc::clone);
        Histogram::from_arc(handle)
    }
}

/// The exporter background task future.
pub type ExporterFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Builder for configuring and installing the CSV recorder.
///
/// Call [`CsvBuilder::with_path`] before building or installing.
#[derive(Debug, Clone)]
pub struct CsvBuilder {
    path: Option<PathBuf>,
    flush_interval: Duration,
    #[cfg(feature = "prometheus")]
    snapshot_source: Option<Arc<dyn SnapshotSource>>,
}

impl Default for CsvBuilder {
    fn default() -> Self {
        Self {
            path: None,
            flush_interval: Duration::from_secs(DEFAULT_FLUSH_INTERVAL_SECS),
            #[cfg(feature = "prometheus")]
            snapshot_source: None,
        }
    }
}

impl CsvBuilder {
    /// Sets the output CSV path.
    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Sets the flush interval for background writers.
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = if interval.is_zero() {
            Duration::from_secs(DEFAULT_FLUSH_INTERVAL_SECS)
        } else {
            interval
        };
        self
    }

    /// Polls the source during periodic and final flushes.
    #[cfg(feature = "prometheus")]
    pub fn with_snapshot_source(mut self, source: impl SnapshotSource + 'static) -> Self {
        self.snapshot_source = Some(Arc::new(source));
        self
    }

    /// Builds the recorder and returns the exporter future.
    pub fn build(self) -> Result<(CsvRecorder, ExporterFuture), BuildError> {
        self.build_recorder_and_exporter()
    }

    /// Builds and installs the recorder.
    ///
    /// This method spawns a background thread that runs a single-threaded Smol executor
    /// to drive the exporter future.
    pub fn install(self) -> Result<(), BuildError> {
        let (recorder, exporter) = self.build_recorder_and_exporter()?;
        metrics::set_global_recorder(recorder).map_err(BuildError::Install)?;

        thread::spawn(move || smol::block_on(exporter));

        Ok(())
    }

    /// Builds and installs the recorder using the current Tokio runtime.
    ///
    /// This method spawns the exporter future onto the Tokio runtime. It must be
    /// called from within a Tokio runtime context.
    #[cfg(feature = "tokio")]
    pub fn install_with_tokio(self) -> Result<(), BuildError> {
        let (recorder, exporter) = self.build_recorder_and_exporter()?;
        metrics::set_global_recorder(recorder).map_err(BuildError::Install)?;

        task::spawn(exporter);

        Ok(())
    }

    fn build_recorder_and_exporter(self) -> Result<(CsvRecorder, ExporterFuture), BuildError> {
        let path = self.path.ok_or(BuildError::MissingPath)?;
        let path = normalize_base_path(path);
        create_output_file(&path)?;

        let file = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(crate::CsvError::from)?;

        let registry = Arc::new(Registry::<Key, AtomicStorage>::atomic());
        let event = Event::new();
        let shutdown = event.listen();
        let exporter = Box::pin(run_write_loop_async(
            csv_async::AsyncWriter::from_writer(File::from(file)),
            Arc::clone(&registry),
            self.flush_interval,
            shutdown,
            #[cfg(feature = "prometheus")]
            self.snapshot_source,
        ));

        let recorder = CsvRecorder::new(registry, event);
        Ok((recorder, exporter))
    }
}

async fn run_write_loop_async(
    mut writer: AsyncWriter<File>,
    registry: Arc<Registry<Key, AtomicStorage>>,
    interval: Duration,
    shutdown: EventListener,
    #[cfg(feature = "prometheus")] snapshot_source: Option<Arc<dyn SnapshotSource>>,
) {
    enum Event {
        Timer,
        Shutdown,
    }

    let mut race = stream::race(
        Timer::interval(interval).map(|_| Event::Timer),
        stream::once_future(shutdown).map(|_| Event::Shutdown),
    );
    loop {
        match race.next().await {
            Some(Event::Timer) => {
                if let Err(error) = write_snapshot(
                    &registry,
                    &mut writer,
                    #[cfg(feature = "prometheus")]
                    snapshot_source.as_deref(),
                )
                .await
                {
                    error!(%error, "csv exporter periodic write failed");
                    return;
                }
                if let Err(error) = writer.flush().await {
                    error!(%error, "csv exporter periodic flush failed");
                    return;
                }
            }
            Some(Event::Shutdown) => {
                if let Err(error) = write_snapshot(
                    &registry,
                    &mut writer,
                    #[cfg(feature = "prometheus")]
                    snapshot_source.as_deref(),
                )
                .await
                {
                    error!(%error, "csv exporter final write failed");
                    return;
                }
                if let Err(error) = writer.flush().await {
                    error!(%error, "csv exporter flush failed");
                }
                break;
            }
            None => break,
        }
    }
}

fn normalize_base_path(path: PathBuf) -> PathBuf {
    let mut path = if path.extension().is_none() {
        path.with_extension("csv")
    } else {
        path
    };

    if path
        .parent()
        .map(|parent| parent.as_os_str().is_empty())
        .unwrap_or(true)
    {
        path = Path::new(".").join(path);
    }

    path
}

fn create_output_file(path: &Path) -> Result<(), CsvError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    smol::block_on(async {
        let mut writer = AsyncWriter::from_writer(File::from(file));
        writer.write_record(HEADER).await?;
        writer.flush().await?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use metrics::Key;
    use tempfile::tempdir;

    use super::*;

    async fn open_append_writer_async(path: &Path) -> Result<AsyncWriter<File>, CsvError> {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(CsvError::from)?;
        Ok(AsyncWriter::from_writer(File::from(file)))
    }

    async fn read_csv(path: &Path) -> Vec<Vec<String>> {
        let file = File::open(path).await.expect("open csv");
        let mut reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .create_reader(file);

        let mut rows = Vec::new();
        let mut records = reader.records();
        while let Some(record) = records.next().await {
            let record = record.expect("record");
            rows.push(record.iter().map(|cell| cell.to_string()).collect());
        }
        rows
    }

    #[test]
    fn write_row_with_no_metrics_writes_only_header() {
        smol::block_on(async {
            let temp_dir = tempdir().expect("tempdir");
            let path = temp_dir.path().join("metrics.csv");
            create_output_file(&path).expect("create file");

            let registry = Registry::<Key, AtomicStorage>::atomic();
            let mut writer = open_append_writer_async(&path).await.expect("open writer");
            write_snapshot(
                &registry,
                &mut writer,
                #[cfg(feature = "prometheus")]
                None,
            )
            .await
            .expect("write row");
            writer.flush().await.expect("flush");

            let rows = read_csv(&path).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    "timestamp_rfc3339".to_string(),
                    "name".to_string(),
                    "kind".to_string(),
                    "labels".to_string(),
                    "value".to_string()
                ]
            );
        })
    }

    #[test]
    fn write_row_writes_header_and_values() {
        smol::block_on(async {
            let temp_dir = tempdir().expect("tempdir");
            let path = temp_dir.path().join("metrics.csv");
            create_output_file(&path).expect("create file");

            let registry = Registry::<Key, AtomicStorage>::atomic();
            let counter_key = Key::from_name("counter_total");
            let gauge_key = Key::from_name("temperature");
            let hist_key = Key::from_name("latency");

            let counter = registry.get_or_create_counter(&counter_key, Arc::clone);
            let gauge = registry.get_or_create_gauge(&gauge_key, Arc::clone);
            let bucket = registry.get_or_create_histogram(&hist_key, Arc::clone);

            counter.fetch_add(2, std::sync::atomic::Ordering::Relaxed);
            gauge.store(1.25_f64.to_bits(), std::sync::atomic::Ordering::Relaxed);
            bucket.push(10.0);
            bucket.push(20.0);

            let mut writer = open_append_writer_async(&path).await.expect("open writer");
            write_snapshot(
                &registry,
                &mut writer,
                #[cfg(feature = "prometheus")]
                None,
            )
            .await
            .expect("write row");
            writer.flush().await.expect("flush");

            let rows = read_csv(&path).await;
            let header = &rows[0];
            assert_eq!(
                *header,
                vec![
                    "timestamp_rfc3339".to_string(),
                    "name".to_string(),
                    "kind".to_string(),
                    "labels".to_string(),
                    "value".to_string()
                ]
            );

            // Scalar metrics are one row each; histograms are one row per statistic.
            let mut scalars: HashMap<String, (String, String, String)> = HashMap::new();
            let mut histogram: HashMap<String, String> = HashMap::new();
            for row in rows.iter().skip(1) {
                assert_eq!(row.len(), 5);
                assert!(!row[0].is_empty(), "timestamp missing");
                if row[1] == "latency" {
                    assert_eq!(row[2], "histogram");
                    histogram.insert(row[3].clone(), row[4].clone());
                } else {
                    scalars.insert(
                        row[1].clone(),
                        (row[2].clone(), row[3].clone(), row[4].clone()),
                    );
                }
            }

            assert_eq!(
                scalars.get("counter_total"),
                Some(&("counter".to_string(), "".to_string(), "2".to_string()))
            );
            assert_eq!(
                scalars.get("temperature"),
                Some(&("gauge".to_string(), "".to_string(), "1.250000".to_string()))
            );
            assert_eq!(histogram.get("stat=count").map(String::as_str), Some("2"));
            assert_eq!(
                histogram.get("stat=min").map(String::as_str),
                Some("10.000000")
            );
            assert_eq!(
                histogram.get("stat=max").map(String::as_str),
                Some("20.000000")
            );
            assert!(histogram.contains_key("stat=q50"), "q50 row missing");

            assert!(bucket.is_empty(), "histogram bucket should be cleared");
        })
    }

    #[cfg(feature = "prometheus")]
    mod snapshot_source_tests {
        use super::*;
        use metrics::Level;
        use prometheus_client::encoding::prometheus_protobuf::prometheus_data_model::{
            Bucket, BucketSpan, Histogram, LabelPair, Metric, MetricFamily, MetricType,
        };

        #[derive(Debug)]
        struct ReceivedSnapshot;

        impl SnapshotSource for ReceivedSnapshot {
            fn snapshot(&self) -> Vec<MetricFamily> {
                vec![MetricFamily {
                    name: "client_latency".into(),
                    r#type: MetricType::Histogram.into(),
                    metric: vec![Metric {
                        label: vec![LabelPair {
                            name: "session_id".into(),
                            value: "7".into(),
                        }],
                        histogram: Some(Histogram {
                            sample_count: 9_007_199_254_740_993,
                            sample_sum: 3.5,
                            schema: 2,
                            zero_threshold: 0.1,
                            zero_count: 1,
                            bucket: vec![Bucket {
                                cumulative_count: 3,
                                upper_bound: 2.0,
                                ..Default::default()
                            }],
                            positive_span: vec![BucketSpan {
                                offset: 1,
                                length: 2,
                            }],
                            positive_delta: vec![3, -1],
                            negative_span: vec![BucketSpan {
                                offset: -1,
                                length: 1,
                            }],
                            negative_count: vec![1.5],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }]
            }
        }

        #[test]
        fn final_flush_combines_local_metrics_and_additional_snapshots() {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("metrics.csv");
            let (recorder, exporter) = CsvBuilder::default()
                .with_path(&path)
                .with_snapshot_source(ReceivedSnapshot)
                .build()
                .unwrap();
            let metadata = Metadata::new(module_path!(), Level::INFO, Some(module_path!()));
            recorder
                .register_counter(&Key::from_name("server_requests"), &metadata)
                .increment(2);
            drop(recorder);
            smol::block_on(exporter);
            smol::block_on(async {
                let rows = read_csv(&path).await;
                assert_eq!(rows.len(), 14);
                assert!(
                    rows.iter()
                        .any(|row| row[1..] == ["server_requests", "counter", "", "2"])
                );
                let mut values = HashMap::new();
                for row in rows.iter().filter(|row| row[1] == "client_latency") {
                    assert_eq!(row[2], "histogram");
                    assert!(values.insert(row[3].clone(), row[4].clone()).is_none());
                }
                for (stat, value) in [
                    ("count", "9007199254740993"),
                    ("sum", "3.5"),
                    ("schema", "2"),
                    ("zero_threshold", "0.1"),
                    ("zero_count", "1"),
                    ("positive_spans", "[BucketSpan { offset: 1, length: 2 }]"),
                    ("positive_deltas", "[3, -1]"),
                    ("positive_counts", "[]"),
                    ("negative_spans", "[BucketSpan { offset: -1, length: 1 }]"),
                    ("negative_deltas", "[]"),
                    ("negative_counts", "[1.5]"),
                ] {
                    assert_eq!(
                        values
                            .remove(&format!("session_id=7|stat={stat}"))
                            .as_deref(),
                        Some(value)
                    );
                }
                let snapshot = ReceivedSnapshot.snapshot();
                let bucket = &snapshot[0].metric[0].histogram.as_ref().unwrap().bucket;
                assert_eq!(
                    values.remove("session_id=7|stat=buckets"),
                    Some(format!("{bucket:?}"))
                );
                assert!(values.is_empty());
            });
        }
    }
}
