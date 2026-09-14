use std::fmt;

use metrics::{Key, Label};
use prometheus_client::encoding::prometheus_protobuf::prometheus_data_model::{
    Histogram, MetricFamily, MetricType,
};

use crate::snapshot::MetricSample;

/// Supplies current aggregates; repeated snapshots do not accumulate in the recorder.
pub trait SnapshotSource: fmt::Debug + Send + Sync {
    fn snapshot(&self) -> Vec<MetricFamily>;
}

pub fn collect_samples(metrics: &[MetricFamily]) -> Vec<MetricSample> {
    let mut rows = Vec::new();
    for metric in metrics {
        for series in &metric.metric {
            let labels: Vec<_> = series
                .label
                .iter()
                .map(|label| Label::new(label.name.clone(), label.value.clone()))
                .collect();
            let key = Key::from_parts(metric.name.clone(), labels);
            match MetricType::try_from(metric.r#type) {
                Ok(MetricType::Counter) => {
                    if let Some(counter) = &series.counter {
                        rows.push(MetricSample::new(&key, "counter", counter.value));
                    }
                }
                Ok(MetricType::Gauge) => {
                    if let Some(gauge) = &series.gauge {
                        rows.push(MetricSample::new(&key, "gauge", gauge.value));
                    }
                }
                Ok(MetricType::Histogram) => {
                    if let Some(histogram) = &series.histogram {
                        for (stat, value) in histogram_values(histogram) {
                            let key = key.with_extra_labels(vec![Label::new("stat", stat)]);
                            rows.push(MetricSample::new(&key, "histogram", value));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    rows
}

fn histogram_values(histogram: &Histogram) -> [(&'static str, String); 12] {
    let count = if histogram.sample_count_float > 0.0 {
        histogram.sample_count_float.to_string()
    } else {
        histogram.sample_count.to_string()
    };
    let zero_count = if histogram.zero_count_float > 0.0 {
        histogram.zero_count_float.to_string()
    } else {
        histogram.zero_count.to_string()
    };
    [
        ("count", count),
        ("sum", histogram.sample_sum.to_string()),
        ("schema", histogram.schema.to_string()),
        ("zero_threshold", histogram.zero_threshold.to_string()),
        ("zero_count", zero_count),
        ("buckets", format!("{:?}", histogram.bucket)),
        ("positive_spans", format!("{:?}", histogram.positive_span)),
        ("positive_deltas", format!("{:?}", histogram.positive_delta)),
        ("positive_counts", format!("{:?}", histogram.positive_count)),
        ("negative_spans", format!("{:?}", histogram.negative_span)),
        ("negative_deltas", format!("{:?}", histogram.negative_delta)),
        ("negative_counts", format!("{:?}", histogram.negative_count)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_counts_take_precedence() {
        let histogram = Histogram {
            sample_count: 9,
            sample_count_float: 2.5,
            zero_count: 8,
            zero_count_float: 1.5,
            ..Default::default()
        };
        let values = histogram_values(&histogram);
        assert_eq!(values[0], ("count", "2.5".into()));
        assert_eq!(values[4], ("zero_count", "1.5".into()));
    }
}
