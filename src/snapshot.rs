use std::{collections::BTreeMap, fmt, sync::atomic::Ordering};

use chrono::{SecondsFormat, Utc};
use csv_async::AsyncWriter;
use fixed::types::I58F6;
use itertools::Itertools;
use metrics::Key;
use metrics_util::{
    registry::{AtomicStorage, Registry},
    storage::AtomicBucket,
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
        let value = encode_histogram(bucket);
        samples.push(MetricSample {
            name: format_name(key),
            kind: "histogram",
            labels: format_labels(key),
            value,
        });
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

fn encode_histogram(bucket: &AtomicBucket<f64>) -> String {
    let mut finite_counts = BTreeMap::new();
    let mut neg_infinite = 0_u64;
    let mut pos_infinite = 0_u64;
    let mut nan = 0_u64;

    bucket.clear_with(|values| {
        for value in values {
            let value = *value;
            if value.is_finite() {
                let key = I58F6::from_num(value);
                let entry = finite_counts.entry(key).or_insert(0);
                *entry += 1;
            } else if value.is_nan() {
                nan += 1;
            } else if value.is_sign_negative() {
                neg_infinite += 1;
            } else {
                pos_infinite += 1;
            }
        }
    });

    let non_finite = [("-inf", neg_infinite), ("inf", pos_infinite), ("NaN", nan)]
        .into_iter()
        .filter(|&(_, count)| count > 0)
        .map(|(label, count)| HistogramEntry::NonFinite { label, count });

    let mut entries = finite_counts
        .into_iter()
        .map(|(value, count)| HistogramEntry::Finite { value, count })
        .chain(non_finite);

    entries.join("|")
}

enum HistogramEntry {
    Finite { value: I58F6, count: u64 },
    NonFinite { label: &'static str, count: u64 },
}

struct LabelEntry<'a> {
    key: &'a str,
    value: &'a str,
}

impl fmt::Display for HistogramEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HistogramEntry::Finite { value, count } => {
                write!(f, "{value:.6}:{count}")
            }
            HistogramEntry::NonFinite { label, count } => write!(f, "{label}:{count}"),
        }
    }
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

    use super::{collect_samples, encode_histogram, format_labels, format_name};

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
        let mut by_name: HashMap<String, (String, String, String)> = HashMap::new();
        for sample in samples {
            by_name.insert(
                sample.name,
                (sample.kind.to_string(), sample.labels.clone(), sample.value),
            );
        }

        assert_eq!(
            by_name.get("counter_total"),
            Some(&("counter".to_string(), "".to_string(), "5".to_string()))
        );
        assert_eq!(
            by_name.get("temperature"),
            Some(&("gauge".to_string(), "".to_string(), "3.500000".to_string()))
        );
        assert_eq!(
            by_name.get("latency"),
            Some(&(
                "histogram".to_string(),
                "".to_string(),
                "1.000000:1|2.000000:1|3.000000:1".to_string()
            ))
        );

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
    fn collect_samples_emits_empty_histogram_value() {
        let registry = Registry::<Key, AtomicStorage>::atomic();
        let hist_key = Key::from_name("latency");
        registry.get_or_create_histogram(&hist_key, Arc::clone);

        let samples = collect_samples(&registry);
        let sample = samples
            .into_iter()
            .find(|sample| sample.name == "latency")
            .expect("latency sample");

        assert_eq!(sample.kind, "histogram");
        assert!(sample.value.is_empty(), "expected empty histogram value");
    }

    #[test]
    fn encode_histogram_clears_bucket() {
        let bucket = Arc::new(AtomicBucket::new());
        bucket.push(10.0);
        bucket.push(20.0);

        let encoded = encode_histogram(&bucket);
        assert_eq!(encoded, "10.000000:1|20.000000:1");

        let second = encode_histogram(&bucket);
        assert!(second.is_empty(), "expected empty output after clear");
        assert!(bucket.is_empty(), "bucket should be empty after clear");
    }

    #[test]
    fn encode_histogram_orders_and_formats_non_finite() {
        let bucket = Arc::new(AtomicBucket::new());
        bucket.push(f64::INFINITY);
        bucket.push(f64::NEG_INFINITY);
        bucket.push(f64::NAN);
        bucket.push(2.0);
        bucket.push(1.0);
        bucket.push(2.0);

        let encoded = encode_histogram(&bucket);
        assert_eq!(encoded, "1.000000:1|2.000000:2|-inf:1|inf:1|NaN:1");
    }
}
