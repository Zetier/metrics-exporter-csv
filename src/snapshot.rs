use std::{fmt, sync::atomic::Ordering};

use chrono::{SecondsFormat, Utc};
use csv_async::AsyncWriter;
use itertools::Itertools;
use metrics::Key;
use metrics_util::{
    registry::{AtomicStorage, Registry},
    storage::{AtomicBucket, Summary},
};
use smol::fs::File;

use crate::{
    error::CsvError,
    util::{format_float, sanitize_component, write_sanitized_component},
};

pub(crate) struct MetricSample {
    pub(crate) name: String,
    pub(crate) kind: &'static str,
    pub(crate) labels: String,
    pub(crate) value: String,
}

pub fn collect_samples(registry: &Registry<Key, AtomicStorage>) -> Vec<MetricSample> {
    let mut samples = Vec::new();

    registry.visit_counters(|key, counter| {
        let value = counter.load(Ordering::Acquire).to_string();
        samples.push(MetricSample {
            name: format_name(key),
            kind: "counter",
            labels: format_labels(key),
            value,
        });
    });

    registry.visit_gauges(|key, gauge| {
        let value = f64::from_bits(gauge.load(Ordering::Acquire));
        samples.push(MetricSample {
            name: format_name(key),
            kind: "gauge",
            labels: format_labels(key),
            value: format_float(value),
        });
    });

    registry.visit_histograms(|key, bucket| {
        let name = format_name(key);
        let base_labels = format_labels(key);
        for (stat, value) in histogram_stats(bucket) {
            let labels = if base_labels.is_empty() {
                format!("stat={stat}")
            } else {
                format!("{base_labels}|stat={stat}")
            };
            samples.push(MetricSample {
                name: name.clone(),
                kind: "histogram",
                labels,
                value,
            });
        }
    });

    samples.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then(a.labels.cmp(&b.labels))
            .then(a.kind.cmp(b.kind))
    });

    samples
}

pub(crate) async fn write_snapshot(
    registry: &Registry<Key, AtomicStorage>,
    writer: &mut AsyncWriter<File>,
) -> Result<(), CsvError> {
    let samples = collect_samples(registry);
    if samples.is_empty() {
        return Ok(());
    }

    let timestamp = current_timestamp();
    for sample in &samples {
        writer
            .write_record([
                timestamp.as_str(),
                sample.name.as_str(),
                sample.kind,
                sample.labels.as_str(),
                sample.value.as_str(),
            ])
            .await?;
    }

    Ok(())
}

fn current_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn format_name(key: &Key) -> String {
    sanitize_component(key.name())
}

fn format_labels(key: &Key) -> String {
    key.labels()
        .map(|label| LabelEntry {
            key: label.key(),
            value: label.value(),
        })
        .join("|")
}

/// Quantiles emitted for each non-empty histogram, alongside `count`/`min`/`max`.
const QUANTILES: [(&str, f64); 5] = [
    ("q50", 0.5),
    ("q90", 0.9),
    ("q95", 0.95),
    ("q99", 0.99),
    ("q999", 0.999),
];

/// Drains `bucket` and summarizes its samples as `(stat, value)` pairs, one per emitted row.
///
/// Finite samples feed a DDSketch ([`Summary`]) from which quantiles, `min`, and `max` are
/// derived; `count` is always emitted (even for an empty bucket) so a registered histogram still
/// produces a row. Non-finite samples cannot enter the sketch and are reported as `-inf`/`inf`/
/// `nan` counts. Every returned `value` is a scalar string, keeping the CSV `value` column numeric.
fn histogram_stats(bucket: &AtomicBucket<f64>) -> Vec<(&'static str, String)> {
    let mut summary = Summary::with_defaults();
    let mut neg_infinite = 0_u64;
    let mut pos_infinite = 0_u64;
    let mut nan = 0_u64;

    bucket.clear_with(|values| {
        for value in values {
            let value = *value;
            if value.is_finite() {
                summary.add(value);
            } else if value.is_nan() {
                nan += 1;
            } else if value.is_sign_negative() {
                neg_infinite += 1;
            } else {
                pos_infinite += 1;
            }
        }
    });

    let mut stats = vec![("count", summary.count().to_string())];

    if !summary.is_empty() {
        stats.push(("min", format_float(summary.min())));
        for (label, quantile) in QUANTILES {
            if let Some(value) = summary.quantile(quantile) {
                stats.push((label, format_float(value)));
            }
        }
        stats.push(("max", format_float(summary.max())));
    }

    for (label, count) in [("-inf", neg_infinite), ("inf", pos_infinite), ("nan", nan)] {
        if count > 0 {
            stats.push((label, count.to_string()));
        }
    }

    stats
}

struct LabelEntry<'a> {
    key: &'a str,
    value: &'a str,
}

impl fmt::Display for LabelEntry<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_sanitized_component(f, self.key)?;
        f.write_str("=")?;
        write_sanitized_component(f, self.value)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use metrics::{Counter, Gauge, Key, Label};
    use metrics_util::{
        registry::{AtomicStorage, Registry},
        storage::AtomicBucket,
    };

    use super::{collect_samples, format_labels, format_name, histogram_stats};

    #[test]
    fn format_name_without_labels() {
        let key = Key::from_name("HTTP Requests");
        assert_eq!(format_name(&key), "http_requests");
    }

    #[test]
    fn format_labels_sanitizes_labels() {
        let labels = vec![
            Label::new("status|code", "2=00"),
            Label::new("method", "GET"),
        ];
        let key = Key::from_parts("HTTP Requests", labels);
        let formatted = format_labels(&key);
        let parts: Vec<_> = formatted.split('|').collect();
        assert_eq!(parts.len(), 2, "expected two label parts");
        assert!(parts.contains(&"status_code=2_00"), "status label missing");
        assert!(parts.contains(&"method=get"), "method label missing");
    }

    #[test]
    fn collect_samples_emits_expected_values() {
        let registry = Registry::<Key, AtomicStorage>::atomic();
        let counter_key = Key::from_name("counter_total");
        let gauge_key = Key::from_name("temperature");
        let hist_key = Key::from_name("latency");

        let counter = registry.get_or_create_counter(&counter_key, Arc::clone);
        let gauge = registry.get_or_create_gauge(&gauge_key, Arc::clone);
        let bucket = registry.get_or_create_histogram(&hist_key, Arc::clone);

        counter.fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        gauge.store(3.5_f64.to_bits(), std::sync::atomic::Ordering::Relaxed);
        bucket.push(1.0);
        bucket.push(2.0);
        bucket.push(3.0);

        let samples = collect_samples(&registry);

        let counter = samples
            .iter()
            .find(|sample| sample.name == "counter_total")
            .expect("counter sample");
        assert_eq!(
            (
                counter.kind,
                counter.labels.as_str(),
                counter.value.as_str()
            ),
            ("counter", "", "5")
        );

        let gauge = samples
            .iter()
            .find(|sample| sample.name == "temperature")
            .expect("gauge sample");
        assert_eq!(
            (gauge.kind, gauge.labels.as_str(), gauge.value.as_str()),
            ("gauge", "", "3.500000")
        );

        // Histogram emits one scalar-valued row per statistic, keyed by `stat=` in labels.
        let histogram: HashMap<String, String> = samples
            .iter()
            .filter(|sample| sample.name == "latency")
            .map(|sample| {
                assert_eq!(sample.kind, "histogram");
                (sample.labels.clone(), sample.value.clone())
            })
            .collect();
        assert_eq!(histogram.get("stat=count").map(String::as_str), Some("3"));
        assert_eq!(
            histogram.get("stat=min").map(String::as_str),
            Some("1.000000")
        );
        assert_eq!(
            histogram.get("stat=max").map(String::as_str),
            Some("3.000000")
        );
        assert!(histogram.contains_key("stat=q50"), "q50 row missing");
        assert!(histogram.contains_key("stat=q99"), "q99 row missing");

        assert!(bucket.is_empty(), "histogram bucket should be cleared");
    }

    #[test]
    fn collect_samples_captures_concurrent_updates() {
        let registry = Registry::<Key, AtomicStorage>::atomic();
        let counter_key = Key::from_name("concurrent_counter");
        let gauge_key = Key::from_name("concurrent_gauge");

        let counter = registry.get_or_create_counter(&counter_key, Arc::clone);
        let gauge = registry.get_or_create_gauge(&gauge_key, Arc::clone);

        let counter_handle = Counter::from_arc(counter);
        let gauge_handle = Gauge::from_arc(gauge);

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

        let samples = collect_samples(&registry);
        let mut by_name: HashMap<String, (String, String, String)> = HashMap::new();
        for sample in samples {
            by_name.insert(
                sample.name,
                (sample.kind.to_string(), sample.labels, sample.value),
            );
        }

        assert_eq!(
            by_name.get("concurrent_counter"),
            Some(&("counter".to_string(), "".to_string(), "4000".to_string()))
        );
        assert_eq!(
            by_name.get("concurrent_gauge"),
            Some(&(
                "gauge".to_string(),
                "".to_string(),
                "4000.000000".to_string()
            ))
        );
    }

    #[test]
    fn collect_samples_emits_empty_histogram_count() {
        let registry = Registry::<Key, AtomicStorage>::atomic();
        let hist_key = Key::from_name("latency");
        registry.get_or_create_histogram(&hist_key, Arc::clone);

        let samples = collect_samples(&registry);
        let rows: Vec<_> = samples
            .iter()
            .filter(|sample| sample.name == "latency")
            .collect();

        // An empty histogram still reports its presence via a single count=0 row.
        assert_eq!(
            rows.len(),
            1,
            "empty histogram should emit only a count row"
        );
        assert_eq!(rows[0].kind, "histogram");
        assert_eq!(rows[0].labels, "stat=count");
        assert_eq!(rows[0].value, "0");
    }

    #[test]
    fn histogram_stats_clears_bucket() {
        let bucket = Arc::new(AtomicBucket::new());
        bucket.push(10.0);
        bucket.push(20.0);

        let stats: HashMap<&str, String> = histogram_stats(&bucket).into_iter().collect();
        assert_eq!(stats.get("count").map(String::as_str), Some("2"));
        assert_eq!(stats.get("min").map(String::as_str), Some("10.000000"));
        assert_eq!(stats.get("max").map(String::as_str), Some("20.000000"));

        // Draining is destructive: a second pass sees an empty bucket (count=0 only).
        let second = histogram_stats(&bucket);
        assert_eq!(second, vec![("count", "0".to_string())]);
        assert!(bucket.is_empty(), "bucket should be empty after clear");
    }

    #[test]
    fn histogram_stats_reports_non_finite() {
        let bucket = Arc::new(AtomicBucket::new());
        bucket.push(f64::INFINITY);
        bucket.push(f64::NEG_INFINITY);
        bucket.push(f64::NAN);
        bucket.push(2.0);
        bucket.push(1.0);
        bucket.push(2.0);

        let stats: HashMap<&str, String> = histogram_stats(&bucket).into_iter().collect();
        // Only the finite samples (1, 2, 2) enter the sketch.
        assert_eq!(stats.get("count").map(String::as_str), Some("3"));
        assert_eq!(stats.get("min").map(String::as_str), Some("1.000000"));
        assert_eq!(stats.get("max").map(String::as_str), Some("2.000000"));
        // Non-finite samples are reported separately as counts.
        assert_eq!(stats.get("-inf").map(String::as_str), Some("1"));
        assert_eq!(stats.get("inf").map(String::as_str), Some("1"));
        assert_eq!(stats.get("nan").map(String::as_str), Some("1"));
    }
}
