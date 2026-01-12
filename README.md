# metrics-exporter-csv

CSV exporter for the `metrics` crate.

## Installation

```toml
[dependencies]
metrics-exporter-csv = "0.1.0"
```

## Usage (Smol runtime)

```rust
use std::{time::Duration};

use metrics_exporter_csv::{CsvBuilder};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let (recorder, exporter) = CsvBuilder::default()
    .with_path("/var/log/metrics.csv")
    .with_flush_interval(Duration::from_secs(3))
    .build()
    .expect("build recorder");

metrics::set_global_recorder(recorder).expect("install recorder");
smol::spawn(exporter);
# Ok(())
# }
```

### Runtime requirement

`CsvBuilder::build` returns an exporter future that must be driven by a Smol executor (for
example via `smol::spawn` or `smol::block_on`). `CsvBuilder::install` spawns a background thread
that runs a single-threaded Smol executor to drive the exporter future and returns once the
thread is started.

### Quick install (background thread)

```rust
use std::{time::Duration};

use metrics_exporter_csv::{CsvBuilder};

CsvBuilder::default()
    .with_path("/var/log/metrics.csv")
    .with_flush_interval(Duration::from_secs(3))
    .install()
    .expect("install recorder");
```

### Tokio install (optional)

Enable the `tokio` feature to spawn the exporter on the current Tokio runtime:

```toml
[dependencies]
metrics-exporter-csv = { version = "0.1.0", features = ["tokio"] }
```

```rust
use std::{time::Duration};

use metrics_exporter_csv::{CsvBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    CsvBuilder::default()
        .with_path("/var/log/metrics.csv")
        .with_flush_interval(Duration::from_secs(3))
        .install_with_tokio()
        .expect("install recorder");

    Ok(())
}
```

## Output format (long-form)

Each flush writes one row per metric:

```text
timestamp_rfc3339,name,kind,labels,value
2026-01-08T12:00:00.123Z,requests_total,counter,method=get,42
2026-01-08T12:00:00.123Z,temperature,gauge,,21.400000
2026-01-08T12:00:00.123Z,latency_ms,histogram,,1.000000:2|2.000000:5|inf:1
```

- `timestamp_rfc3339` is UTC with millisecond precision.
- `kind` is one of `counter`, `gauge`, or `histogram`.
- `name` is the sanitized metric name.
- `labels` is a `|`-delimited list of `key=value` pairs (empty when no labels).
- `value` is formatted as:
  - Counters: integer value at flush time.
  - Gauges: fixed 6-decimal float (rounded).
  - Histograms: `value:count` pairs delimited by `|`. Finite values are sorted numerically.
    Non-finite values are appended as `-inf`, `inf`, and `NaN`. If no samples were recorded in
    the interval, the histogram `value` is empty.

### Name and label encoding

The `name` column is the sanitized metric name, and `labels` encodes label key/value pairs:

```text
http_requests_total
method=get|status=200
```

Labels are emitted in the order provided by the metrics key (no additional sorting).
Components are lowercased, and whitespace plus `|` and `=` characters are normalized to
underscores. Other characters are preserved.

## Flush semantics

- The recorder snapshots all known metrics every flush interval (default: 3 seconds).
- Counters and gauges are sampled at flush time.
- Histograms are **cleared** on each snapshot, so histogram counts represent the samples
  recorded since the previous flush interval.

## Deterministic shutdown

Drop the recorder to request a final flush. The exporter future should be allowed to run until it
completes.

## Multi-exporter composition

When combining multiple recorders, build the CSV recorder and fan out with `metrics-util`:

```rust
use metrics_exporter_csv::{CsvBuilder};
use metrics_util::layers::{FanoutBuilder};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let (csv, exporter) = CsvBuilder::default()
    .with_path("metrics.csv")
    .build()
    .expect("create recorder");
let other = /* another recorder */;

let recorder = FanoutBuilder::new()
    .add_recorder(csv)
    .add_recorder(other)
    .build();

metrics::set_global_recorder(recorder).expect("set recorder");
smol::spawn(exporter);
# Ok(())
# }
```

## Notes

- Output is long-form CSV with a fixed header: `timestamp_rfc3339,name,kind,labels,value`.
- The `name` field is the sanitized metric name. The `labels` field contains `key=value` pairs
  delimited by `|` in the order provided by the metrics key. Components are lowercased and
  whitespace plus `|`/`=` are normalized to underscores.
- Histograms are encoded in the `value` field as `value:count` pairs
  delimited by `|` (values formatted to 6 decimals; non-finite values as
  `-inf`, `inf`, `NaN`).
- If the path has no extension, `.csv` is appended.
- The base path must not already exist; creation fails with an error if it does. The base file is
  created when the recorder is constructed and the header is written once.
