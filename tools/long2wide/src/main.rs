use std::{
    collections::{BTreeSet, HashMap},
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Parser;
use csv::StringRecord;

#[derive(Debug, Parser)]
#[command(about = "Convert metrics-exporter-csv long-form CSVs into a wide-form CSV")]
struct Args {
    /// Input CSV files (repeatable) or a glob pattern.
    #[arg(long = "input")]
    inputs: Vec<String>,

    /// Directory to scan for CSV files (lexical order).
    #[arg(long = "input-dir")]
    input_dir: Option<String>,

    /// Output CSV file path.
    #[arg(long = "output")]
    output: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let inputs = resolve_inputs(&args)?;
    convert_long_to_wide(&inputs, Path::new(&args.output))
}

const EXPECTED_HEADER: [&str; 5] = ["timestamp_rfc3339", "name", "kind", "labels", "value"];

#[derive(Debug, Clone)]
struct CsvRow {
    timestamp: String,
    name: String,
    kind: String,
    labels: String,
    value: String,
}

fn resolve_inputs(args: &Args) -> Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = Vec::new();

    for input in &args.inputs {
        paths.extend(expand_input(input)?);
    }

    if let Some(dir) = &args.input_dir {
        paths.extend(scan_dir_for_csvs(dir)?);
    }

    if paths.is_empty() {
        anyhow::bail!("no input files provided (use --input or --input-dir)");
    }

    sort_and_dedup_paths(&mut paths);

    Ok(paths)
}

fn expand_input(input: &str) -> Result<Vec<PathBuf>> {
    if contains_glob(input) {
        let mut matches: Vec<PathBuf> = glob::glob(input)
            .with_context(|| format!("invalid glob pattern: {input}"))?
            .filter_map(|entry| entry.ok())
            .collect();
        if matches.is_empty() {
            anyhow::bail!("glob pattern matched no files: {input}");
        }
        sort_and_dedup_paths(&mut matches);
        return Ok(matches);
    }

    let path = PathBuf::from(input);
    if path.is_file() {
        Ok(vec![path])
    } else if path.is_dir() {
        anyhow::bail!("input '{input}' is a directory; use --input-dir instead");
    } else {
        anyhow::bail!("input file does not exist: {input}");
    }
}

fn contains_glob(input: &str) -> bool {
    input.contains('*') || input.contains('?') || input.contains('[')
}

fn scan_dir_for_csvs(dir: &str) -> Result<Vec<PathBuf>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read dir {dir}"))? {
        let entry = entry?;
        let path = entry.path();
        if path.extension() == Some(OsStr::new("csv")) && path.is_file() {
            entries.push(path);
        }
    }
    if entries.is_empty() {
        anyhow::bail!("no .csv files found in directory: {dir}");
    }
    sort_and_dedup_paths(&mut entries);
    Ok(entries)
}

fn sort_and_dedup_paths(paths: &mut Vec<PathBuf>) {
    paths.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
    paths.dedup_by(|a, b| a.to_string_lossy() == b.to_string_lossy());
}

fn convert_long_to_wide(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let columns = discover_columns(inputs)?;
    write_wide_csv(inputs, output, &columns)
}

fn discover_columns(inputs: &[PathBuf]) -> Result<Vec<String>> {
    let mut columns = BTreeSet::new();
    let mut last_ts = None;

    for_each_row(inputs, |row| {
        let ts = parse_timestamp(&row.timestamp)?;
        if let Some(prev) = last_ts {
            if ts < prev {
                anyhow::bail!(
                    "timestamps are not sorted; saw {} before {}",
                    prev,
                    row.timestamp
                );
            }
        }
        last_ts = Some(ts);

        let series_key = build_series_key(&row.name, &row.labels);
        columns.insert(series_key);
        Ok(())
    })?;

    Ok(columns.into_iter().collect())
}

fn write_wide_csv(inputs: &[PathBuf], output: &Path, columns: &[String]) -> Result<()> {
    let mut writer = csv::Writer::from_path(output)
        .with_context(|| format!("open output file {}", output.display()))?;

    let mut header = Vec::with_capacity(columns.len() + 1);
    header.push(EXPECTED_HEADER[0].to_string());
    header.extend(columns.iter().cloned());
    writer.write_record(&header)?;

    let mut last_ts = None;
    let mut current_ts = None;
    let mut current_ts_string = String::new();
    let mut current_row: HashMap<String, String> = HashMap::new();

    for_each_row(inputs, |row| {
        let ts = parse_timestamp(&row.timestamp)?;
        if let Some(prev) = last_ts {
            if ts < prev {
                anyhow::bail!(
                    "timestamps are not sorted; saw {} before {}",
                    prev,
                    row.timestamp
                );
            }
        }
        last_ts = Some(ts);

        if let Some(cur) = current_ts {
            if ts > cur {
                write_row(&mut writer, &current_ts_string, columns, &current_row)?;
                current_row.clear();
                current_ts = Some(ts);
                current_ts_string = row.timestamp.clone();
            }
        }

        if current_ts.is_none() {
            current_ts = Some(ts);
            current_ts_string = row.timestamp.clone();
        }

        let series_key = build_series_key(&row.name, &row.labels);
        if current_row.contains_key(&series_key) {
            anyhow::bail!(
                "duplicate timestamp+series: {} / {}",
                row.timestamp,
                series_key
            );
        }
        current_row.insert(series_key, row.value);

        Ok(())
    })?;

    if current_ts.is_some() {
        write_row(&mut writer, &current_ts_string, columns, &current_row)?;
    }

    writer.flush()?;
    Ok(())
}

fn write_row(
    writer: &mut csv::Writer<std::fs::File>,
    timestamp: &str,
    columns: &[String],
    row_values: &HashMap<String, String>,
) -> Result<()> {
    let mut record = Vec::with_capacity(columns.len() + 1);
    record.push(timestamp.to_string());
    for column in columns {
        record.push(row_values.get(column).cloned().unwrap_or_default());
    }
    writer.write_record(&record)?;
    Ok(())
}

fn build_series_key(name: &str, labels: &str) -> String {
    if labels.is_empty() {
        name.to_string()
    } else {
        format!("{name}|{labels}")
    }
}

fn for_each_row(inputs: &[PathBuf], mut f: impl FnMut(CsvRow) -> Result<()>) -> Result<()> {
    for path in inputs {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(path)
            .with_context(|| format!("open input file {}", path.display()))?;

        let mut records = reader.records();
        let mut is_first_record = true;

        while let Some(record) = records.next() {
            let record = record?;
            if record.is_empty() {
                continue;
            }

            if is_first_record && is_header(&record) {
                is_first_record = false;
                continue;
            }
            is_first_record = false;

            if record.len() != EXPECTED_HEADER.len() {
                anyhow::bail!(
                    "expected {} columns but found {} in {}",
                    EXPECTED_HEADER.len(),
                    record.len(),
                    path.display()
                );
            }

            let row = CsvRow {
                timestamp: record[0].to_string(),
                name: record[1].to_string(),
                kind: record[2].to_string(),
                labels: record[3].to_string(),
                value: record[4].to_string(),
            };

            validate_kind(&row.kind)?;
            f(row)?;
        }
    }
    Ok(())
}

fn is_header(record: &StringRecord) -> bool {
    record.len() == EXPECTED_HEADER.len() && record.iter().zip(EXPECTED_HEADER).all(|(a, b)| a == b)
}

fn parse_timestamp(timestamp: &str) -> Result<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .with_context(|| format!("invalid timestamp: {timestamp}"))
}

fn validate_kind(kind: &str) -> Result<()> {
    match kind {
        "counter" | "gauge" | "histogram" => Ok(()),
        _ => anyhow::bail!("invalid kind: {kind}"),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn write_file(path: &Path, contents: &str) {
        let mut file = std::fs::File::create(path).expect("create file");
        file.write_all(contents.as_bytes()).expect("write file");
    }

    #[test]
    fn converts_basic_rows() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let input = temp_dir.path().join("metrics.csv");
        let output = temp_dir.path().join("wide.csv");

        write_file(
            &input,
            "timestamp_rfc3339,name,kind,labels,value\n\
             2026-01-08T12:00:00.123Z,requests_total,counter,method=get,42\n\
             2026-01-08T12:00:00.123Z,temperature,gauge,,21.400000\n\
             2026-01-08T12:00:03.123Z,requests_total,counter,method=get,43\n",
        );

        convert_long_to_wide(&[input], &output).expect("convert");

        let out = std::fs::read_to_string(&output).expect("read output");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines[0],
            "timestamp_rfc3339,requests_total|method=get,temperature"
        );
        assert_eq!(lines[1], "2026-01-08T12:00:00.123Z,42,21.400000");
        assert_eq!(lines[2], "2026-01-08T12:00:03.123Z,43,");
    }

    #[test]
    fn handles_rotated_files_with_and_without_header() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let older = temp_dir.path().join("metrics.csv.1");
        let newer = temp_dir.path().join("metrics.csv");
        let output = temp_dir.path().join("wide.csv");

        write_file(
            &older,
            "2026-01-08T12:00:00.123Z,requests_total,counter,method=get,1\n",
        );

        write_file(
            &newer,
            "timestamp_rfc3339,name,kind,labels,value\n\
             2026-01-08T12:00:03.123Z,requests_total,counter,method=get,2\n",
        );

        convert_long_to_wide(&[older, newer], &output).expect("convert");

        let out = std::fs::read_to_string(&output).expect("read output");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "timestamp_rfc3339,requests_total|method=get");
        assert_eq!(lines[1], "2026-01-08T12:00:00.123Z,1");
        assert_eq!(lines[2], "2026-01-08T12:00:03.123Z,2");
    }

    #[test]
    fn errors_on_duplicate_timestamp_series() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let input = temp_dir.path().join("metrics.csv");
        let output = temp_dir.path().join("wide.csv");

        write_file(
            &input,
            "timestamp_rfc3339,name,kind,labels,value\n\
             2026-01-08T12:00:00.123Z,requests_total,counter,method=get,1\n\
             2026-01-08T12:00:00.123Z,requests_total,counter,method=get,2\n",
        );

        let err = convert_long_to_wide(&[input], &output).expect_err("should error");
        assert!(
            err.to_string().contains("duplicate timestamp+series"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn errors_on_out_of_order_timestamps() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let input = temp_dir.path().join("metrics.csv");
        let output = temp_dir.path().join("wide.csv");

        write_file(
            &input,
            "timestamp_rfc3339,name,kind,labels,value\n\
             2026-01-08T12:00:03.123Z,requests_total,counter,method=get,1\n\
             2026-01-08T12:00:00.123Z,requests_total,counter,method=get,2\n",
        );

        let err = convert_long_to_wide(&[input], &output).expect_err("should error");
        assert!(
            err.to_string().contains("timestamps are not sorted"),
            "unexpected error: {err}"
        );
    }
}
