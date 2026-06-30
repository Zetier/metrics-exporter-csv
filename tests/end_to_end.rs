use std::time::Duration;

use metrics::{counter, gauge, histogram, with_local_recorder};
use metrics_exporter_csv::CsvBuilder;
use smol::{fs::File, prelude::*};
use tempfile::tempdir;

#[test]
fn local_recorder_writes_metrics_on_drop() {
    smol::block_on(async {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("metrics.csv");

        let (recorder, exporter) = CsvBuilder::default()
            .with_path(path.clone())
            .with_flush_interval(Duration::from_secs(60))
            .build()
            .expect("recorder");

        let handle = smol::spawn(exporter);

        with_local_recorder(&recorder, || {
            let counter = counter!("requests_total");
            counter.increment(3);

            let gauge = gauge!("temperature");
            gauge.set(2.5);

            let histogram = histogram!("latency");
            histogram.record(1.0);
        });

        drop(recorder);
        handle.await;

        let file = File::open(&path).await.expect("open csv");
        let mut reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .create_reader(file);
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut records = reader.records();
        while let Some(record) = records.next().await {
            let record = record.expect("record");
            rows.push(record.iter().map(|cell| cell.to_string()).collect());
        }

        assert!(!rows.is_empty(), "missing csv rows");
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

        let mut saw_counter = false;
        let mut saw_gauge = false;
        let mut saw_hist = false;
        for row in rows.iter().skip(1) {
            if row[1] == "requests_total" && row[2] == "counter" && row[3].is_empty() {
                saw_counter = true;
            }
            if row[1] == "temperature" && row[2] == "gauge" && row[3].is_empty() {
                saw_gauge = true;
            }
            if row[1] == "latency"
                && row[2] == "histogram"
                && row[3] == "stat=count"
                && row[4] == "1"
            {
                saw_hist = true;
            }
        }

        assert!(saw_counter, "counter row missing");
        assert!(saw_gauge, "gauge row missing");
        assert!(saw_hist, "histogram row missing");
    })
}
