use std::time::Duration;

use metrics::counter;
use metrics_exporter_csv::{BuildError, CsvBuilder};
use smol::{fs::File, prelude::*};

fn read_rows(path: &std::path::Path) -> Vec<Vec<String>> {
    smol::block_on(async {
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
    })
}

#[test]
fn install_writes_metrics_and_rejects_second_install() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("metrics.csv");

    CsvBuilder::default()
        .with_path(&path)
        .with_flush_interval(Duration::from_millis(10))
        .install()
        .expect("install recorder");

    counter!("install_counter").increment(1);

    std::thread::sleep(Duration::from_millis(30));

    let rows = read_rows(&path);
    assert!(rows.len() >= 2, "missing rows");
    let saw_counter = rows
        .iter()
        .skip(1)
        .any(|row| row.len() == 5 && row[1] == "install_counter");
    assert!(saw_counter, "counter row missing");

    let other_path = temp_dir.path().join("other.csv");
    let err = CsvBuilder::default()
        .with_path(&other_path)
        .install()
        .expect_err("expected install error");

    assert!(matches!(err, BuildError::Install(_)));
}
