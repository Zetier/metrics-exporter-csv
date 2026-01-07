# metrics-exporter-csv

CSV exporter for the `metrics` crate.

## Usage

```rust
use std::time::Duration;

use metrics_exporter_csv::CsvRecorder;

let recorder = CsvRecorder::with_flush_interval("/var/log/metrics.csv", Duration::from_secs(3));
metrics::set_global_recorder(recorder).expect("failed to install recorder");
```

## Deterministic shutdown

If you want to ensure a final flush on shutdown, wrap the recorder with
`metrics_util::RecoverableRecorder` and drop the handle:

```rust
use metrics_exporter_csv::CsvRecorder;
use metrics_util::RecoverableRecorder;

let recorder = CsvRecorder::new("/var/log/metrics.csv");
let handle = RecoverableRecorder::new(recorder)
    .install()
    .expect("failed to install recorder");

drop(handle);
```

## Multi-exporter composition

When combining multiple recorders, build the CSV recorder and fan out with `metrics-util`:

```rust
use metrics_exporter_csv::CsvRecorder;
use metrics_util::layers::FanoutBuilder;

let csv = CsvRecorder::new("metrics.csv");
let other = /* another recorder */;

let recorder = FanoutBuilder::new()
    .add_recorder(csv)
    .add_recorder(other)
    .build();

metrics::set_global_recorder(recorder).expect("set recorder");
```

## Notes

- Column names are derived from metric name and labels; labels are appended as `__key_value` segments in label-key order. Components are lowercased and whitespace is normalized to underscores. Columns are ordered lexicographically by full column name.
- Histograms emit fixed summary columns: count, min, max, p50, p90, p99.
- Quantiles are approximate (using `metrics_util::storage::Summary` defaults).
- If the path has no extension, `.csv` is appended.
- Rotation uses `simple-file-rotation`; rotated files are named like `metrics.1.csv`, `metrics.2.csv`, etc.
