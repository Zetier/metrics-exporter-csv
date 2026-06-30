use std::time::Duration;

use metrics::{
    Level, Metadata, Recorder, Unit, counter, describe_counter, describe_gauge, describe_histogram,
    gauge, histogram,
};
use smol::{fs::File, prelude::*};

use metrics_exporter_csv::CsvBuilder;

async fn read_rows(path: &std::path::Path) -> Vec<Vec<String>> {
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

fn assert_timestamp_format(timestamp: &str) {
    assert!(timestamp.ends_with('Z'), "timestamp missing Z suffix");
    let Some(dot) = timestamp.rfind('.') else {
        panic!("timestamp missing milliseconds");
    };
    let millis = &timestamp[dot + 1..timestamp.len() - 1];
    assert_eq!(millis.len(), 3, "timestamp millis length");
}

async fn run_with_exporter(
    path: &std::path::Path,
    interval: Duration,
    f: impl FnOnce(&metrics_exporter_csv::CsvRecorder),
) {
    let (recorder, exporter) = CsvBuilder::default()
        .with_path(path)
        .with_flush_interval(interval)
        .build()
        .expect("recorder");

    let handle = smol::spawn(exporter);
    f(&recorder);
    drop(recorder);
    handle.await;
}

#[test]
fn writes_header_and_timestamp() {
    smol::block_on(async {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("metrics.csv");

        run_with_exporter(&path, Duration::from_millis(10), |recorder| {
            metrics::with_local_recorder(recorder, || {
                describe_counter!("request rate", Unit::Count, "requests");
                describe_gauge!("cpu usage", Unit::Percent, "cpu");
                let req = counter!("request rate", "method" => "GET");
                req.increment(1);
                let cpu = gauge!("cpu usage");
                cpu.set(42.5);
            });
        })
        .await;

        let rows = read_rows(&path).await;
        assert!(rows.len() >= 3);
        let header = &rows[0];
        assert_eq!(
            header,
            &vec![
                "timestamp_rfc3339".to_string(),
                "name".to_string(),
                "kind".to_string(),
                "labels".to_string(),
                "value".to_string()
            ]
        );

        let mut saw_counter = false;
        let mut saw_gauge = false;
        for row in rows.iter().skip(1) {
            assert_eq!(row.len(), 5);
            assert_timestamp_format(&row[0]);
            if row[1] == "request_rate" && row[2] == "counter" && row[3] == "method=get" {
                assert_eq!(row[4], "1");
                saw_counter = true;
            }
            if row[1] == "cpu_usage" && row[2] == "gauge" && row[3].is_empty() {
                assert_eq!(row[4], "42.500000");
                saw_gauge = true;
            }
        }
        assert!(saw_counter, "counter row missing");
        assert!(saw_gauge, "gauge row missing");
    })
}

#[test]
fn writes_multiple_metrics_to_single_file() {
    smol::block_on(async {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("metrics.csv");

        run_with_exporter(&path, Duration::from_millis(10), |recorder| {
            metrics::with_local_recorder(recorder, || {
                let counter = counter!("first_metric");
                counter.increment(1);
            });

            metrics::with_local_recorder(recorder, || {
                let counter = counter!("first_metric");
                counter.increment(1);
                let gauge = gauge!("second_metric");
                gauge.set(std::f64::consts::PI);
            });
        })
        .await;

        let rows = read_rows(&path).await;
        assert!(!rows.is_empty());
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

        let mut saw_first = false;
        let mut saw_second = false;
        for row in rows.iter().skip(1) {
            if row[1] == "first_metric" {
                saw_first = true;
            }
            if row[1] == "second_metric" {
                saw_second = true;
            }
        }

        assert!(saw_first, "first_metric row missing");
        assert!(saw_second, "second_metric row missing");
    })
}

#[test]
fn labels_affect_labels() {
    smol::block_on(async {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("metrics.csv");

        run_with_exporter(&path, Duration::from_millis(10), |recorder| {
            metrics::with_local_recorder(recorder, || {
                let counter = counter!("ordered_labels", "b" => "2", "a" => "1");
                counter.increment(1);
            });
        })
        .await;

        let rows = read_rows(&path).await;
        let mut saw_label = false;
        for row in rows.iter().skip(1) {
            if row[1] == "ordered_labels" && (row[3] == "a=1|b=2" || row[3] == "b=2|a=1") {
                assert_eq!(row[2], "counter");
                assert_eq!(row[4], "1");
                saw_label = true;
            }
        }
        assert!(saw_label, "labeled row missing");
    })
}

#[test]
fn histogram_values_are_encoded() {
    smol::block_on(async {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("metrics.csv");

        run_with_exporter(&path, Duration::from_millis(10), |recorder| {
            metrics::with_local_recorder(recorder, || {
                describe_histogram!("latency", Unit::Milliseconds, "latency");
                let hist = histogram!("latency");
                hist.record(1.0);
                hist.record(3.0);
            });
        })
        .await;

        let rows = read_rows(&path).await;
        let mut histogram = std::collections::HashMap::new();
        for row in rows.iter().skip(1) {
            if row[1] == "latency" && row[2] == "histogram" {
                histogram.insert(row[3].clone(), row[4].clone());
            }
        }
        assert_eq!(histogram.get("stat=count").map(String::as_str), Some("2"));
        assert_eq!(
            histogram.get("stat=min").map(String::as_str),
            Some("1.000000")
        );
        assert_eq!(
            histogram.get("stat=max").map(String::as_str),
            Some("3.000000")
        );
        assert!(histogram.contains_key("stat=q50"), "q50 row missing");
    })
}

#[test]
fn concurrent_updates_are_captured() {
    smol::block_on(async {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("metrics.csv");

        let (recorder, exporter) = CsvBuilder::default()
            .with_path(&path)
            .with_flush_interval(Duration::from_secs(60))
            .build()
            .expect("recorder");
        let handle = smol::spawn(exporter);

        let recorder = std::sync::Arc::new(recorder);
        let metadata = Metadata::new("test", Level::INFO, None);
        let counter_handle =
            recorder.register_counter(&metrics::Key::from_name("concurrent_counter"), &metadata);
        let gauge_handle =
            recorder.register_gauge(&metrics::Key::from_name("concurrent_gauge"), &metadata);

        let mut threads = Vec::new();
        for _ in 0..4 {
            let counter_handle = counter_handle.clone();
            let gauge_handle = gauge_handle.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    counter_handle.increment(1);
                    gauge_handle.increment(1.0);
                }
            }));
        }

        for thread in threads {
            thread.join().expect("thread");
        }

        drop(recorder);
        handle.await;

        let rows = read_rows(&path).await;
        let mut saw_counter = false;
        let mut saw_gauge = false;
        for row in rows.iter().skip(1) {
            if row[1] == "concurrent_counter" && row[2] == "counter" {
                saw_counter = true;
            }
            if row[1] == "concurrent_gauge" && row[2] == "gauge" {
                saw_gauge = true;
            }
        }

        assert!(saw_counter, "counter row missing");
        assert!(saw_gauge, "gauge row missing");
    })
}
