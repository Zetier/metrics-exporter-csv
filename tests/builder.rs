use std::io;

use metrics_exporter_csv::{BuildError, CsvBuilder, CsvError};
use tempfile::tempdir;

#[test]
fn build_requires_path() {
    let err = CsvBuilder::default().build().err().expect("missing path");
    assert!(matches!(err, BuildError::MissingPath));
}

#[test]
fn install_requires_path() {
    let err = CsvBuilder::default().install().expect_err("missing path");
    assert!(matches!(err, BuildError::MissingPath));
}

#[test]
fn build_creates_file_immediately() {
    let temp_dir = tempdir().expect("tempdir");
    let path = temp_dir.path().join("metrics.csv");

    let (recorder, exporter) = CsvBuilder::default()
        .with_path(&path)
        .build()
        .expect("recorder");

    assert!(path.exists(), "expected file to be created");
    drop(recorder);
    drop(exporter);

    let contents = std::fs::read_to_string(&path).expect("read file");
    assert!(
        contents.starts_with("timestamp_rfc3339,name,kind,labels,value"),
        "header missing"
    );
}

#[test]
fn build_fails_if_path_exists() {
    let temp_dir = tempdir().expect("tempdir");
    let path = temp_dir.path().join("metrics.csv");
    std::fs::write(&path, "existing").expect("write file");

    let err = CsvBuilder::default()
        .with_path(&path)
        .build()
        .err()
        .expect("expected error");

    match err {
        BuildError::Csv(CsvError::Io(err)) => {
            assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
