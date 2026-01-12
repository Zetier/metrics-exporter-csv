# long2wide

Tooling to convert long-form CSVs produced by `metrics-exporter-csv` into a wide-form CSV.

## Input schema

The converter expects the long-form header:

```
timestamp_rfc3339,name,kind,labels,value
```

Wide-form columns are derived from `name` and `labels`, joined as `name|labels` when labels are
present (otherwise just `name`).

## Usage
```
long2wide --input "metrics*.csv" --output metrics-wide.csv
```

You can also provide `--input` multiple times or use `--input-dir` to scan a directory for CSVs.
