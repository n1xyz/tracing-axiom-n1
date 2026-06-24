use std::collections::BTreeMap;

use crate::proto::opentelemetry::proto::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality as ProtoAggregationTemporality, Gauge,
        Histogram, HistogramDataPoint, Metric as ProtoMetric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
    },
    resource::v1::Resource,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Metric {
    pub name: String,
    pub description: String,
    pub unit: MetricUnit,
    pub data: MetricData,
    pub attrs: BTreeMap<String, AttrValue>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum MetricData {
    Gauge {
        value: MetricValue,
    },
    Sum {
        temporality: AggregationTemporality,
        monotonic: bool,
        value: MetricValue,
    },
    Histogram {
        temporality: AggregationTemporality,
        count: u64,
        sum: Option<f64>,
        bucket_counts: Vec<u64>,
        explicit_bounds: Vec<f64>,
        min: Option<f64>,
        max: Option<f64>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum AttrValue {
    Str(String),
    I64(i64),
    F(f64),
    Bool(bool),
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize,
)]
pub enum AggregationTemporality {
    Delta,
    Cumulative,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub enum MetricValue {
    F64(f64),
    I64(i64),
}

impl AttrValue {
    pub fn as_proto(self) -> AnyValue {
        match self {
            Self::Str(s) => {
                AnyValue { value: Some(any_value::Value::StringValue(s)) }
            }
            Self::I64(i) => {
                AnyValue { value: Some(any_value::Value::IntValue(i)) }
            }
            Self::F(f) => {
                AnyValue { value: Some(any_value::Value::DoubleValue(f)) }
            }
            Self::Bool(b) => {
                AnyValue { value: Some(any_value::Value::BoolValue(b)) }
            }
        }
    }
}

impl MetricValue {
    pub fn as_proto(self) -> number_data_point::Value {
        match self {
            Self::F64(f) => number_data_point::Value::AsDouble(f),
            Self::I64(i) => number_data_point::Value::AsInt(i),
        }
    }
}

impl AggregationTemporality {
    pub fn as_proto(self) -> ProtoAggregationTemporality {
        match self {
            Self::Delta => ProtoAggregationTemporality::Delta,
            Self::Cumulative => ProtoAggregationTemporality::Cumulative,
        }
    }
}

impl Metric {
    pub fn as_proto(self, time_unix_nano: u64) -> ProtoMetric {
        let attributes = self
            .attrs
            .into_iter()
            .map(|(key, value)| KeyValue {
                key,
                value: Some(value.as_proto()),
                ..Default::default()
            })
            .collect();

        let data = match self.data {
            MetricData::Gauge { value } => {
                let data_point = NumberDataPoint {
                    attributes,
                    time_unix_nano,
                    value: Some(value.as_proto()),
                    ..Default::default()
                };
                metric::Data::Gauge(Gauge { data_points: vec![data_point] })
            }
            MetricData::Sum { temporality, monotonic, value } => {
                let data_point = NumberDataPoint {
                    attributes,
                    time_unix_nano,
                    value: Some(value.as_proto()),
                    ..Default::default()
                };
                metric::Data::Sum(Sum {
                    data_points: vec![data_point],
                    aggregation_temporality: temporality.as_proto() as i32,
                    is_monotonic: monotonic,
                })
            }
            MetricData::Histogram {
                temporality,
                count,
                sum,
                bucket_counts,
                explicit_bounds,
                min,
                max,
            } => {
                let data_point = HistogramDataPoint {
                    attributes,
                    time_unix_nano,
                    count,
                    sum,
                    bucket_counts,
                    explicit_bounds,
                    min,
                    max,
                    ..Default::default()
                };
                metric::Data::Histogram(Histogram {
                    data_points: vec![data_point],
                    aggregation_temporality: temporality.as_proto() as i32,
                })
            }
        };

        ProtoMetric {
            name: self.name,
            description: self.description,
            unit: self.unit.as_str().to_string(),
            data: Some(data),
            ..Default::default()
        }
    }
}

pub fn metrics_to_proto(
    metrics: Vec<Metric>,
    time_unix_nano: u64,
    resource_attrs: Option<BTreeMap<String, AttrValue>>,
) -> ExportMetricsServiceRequest {
    let proto_metrics =
        metrics.into_iter().map(|m| m.as_proto(time_unix_nano)).collect();

    let scope_metrics = vec![ScopeMetrics {
        scope: Some(InstrumentationScope {
            name: concat!(env!("CARGO_PKG_NAME"), "/spanmetrics").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            ..Default::default()
        }),
        metrics: proto_metrics,
        ..Default::default()
    }];

    let resource = resource_attrs.map(|attrs| Resource {
        attributes: attrs
            .into_iter()
            .map(|(k, v)| KeyValue {
                key: k,
                value: Some(v.as_proto()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    });

    let resource_metrics =
        vec![ResourceMetrics { resource, scope_metrics, ..Default::default() }];

    ExportMetricsServiceRequest { resource_metrics }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize,
)]
pub enum MetricUnit {
    KibibytesPerSecond,
    KilobytesPerSecond,
    KilobitsPerSecond,
    BytesPerSecond,

    Kibibytes,
    Kilobytes,
    Kilobits,

    Bytes,
    Bits,

    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,

    Count,
    CountPerSecond,
    Percent,
    Ratio,

    Packets,
    PacketsPerSecond,
    Requests,
    RequestsPerSecond,

    Bool,
    Unknown,
}

impl MetricUnit {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::KibibytesPerSecond => "KiB/s",
            Self::KilobytesPerSecond => "KB/s",
            Self::KilobitsPerSecond => "Kbit/s",
            Self::BytesPerSecond => "B/s",

            Self::Kibibytes => "KiB",
            Self::Kilobytes => "KB",
            Self::Kilobits => "Kbit",

            Self::Bytes => "B",
            Self::Bits => "bit",

            Self::Seconds => "s",
            Self::Milliseconds => "ms",
            Self::Microseconds => "us",
            Self::Nanoseconds => "ns",

            Self::Count => "1",
            Self::CountPerSecond => "1/s",
            Self::Percent => "%",
            Self::Ratio => "1",

            Self::Packets => "{packet}",
            Self::PacketsPerSecond => "{packet}/s",
            Self::Requests => "{request}",
            Self::RequestsPerSecond => "{request}/s",

            Self::Bool => "1",
            Self::Unknown => "",
        }
    }
}
