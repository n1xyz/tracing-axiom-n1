use std::collections::BTreeMap;

use crate::proto::opentelemetry::proto::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality as ProtoAggregationTemporality, Gauge,
        Metric as ProtoMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
        Sum, metric, number_data_point,
    },
    resource::v1::Resource,
};

pub struct Metric {
    pub name: String,
    pub description: String,
    pub unit: MetricUnit,
    pub data: MetricData,
    pub value: MetricValue,
    pub attrs: BTreeMap<String, AttrValue>,
}

pub enum MetricData {
    Gauge,
    Sum { temporality: AggregationTemporality, monotonic: bool },
}

pub enum AttrValue {
    Str(String),
    Uz(u64),
    F(f64),
}

pub enum AggregationTemporality {
    Delta,
    Cumulative,
}

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
            Self::Uz(u) => {
                AnyValue { value: Some(any_value::Value::IntValue(u as i64)) }
            }
            Self::F(f) => {
                AnyValue { value: Some(any_value::Value::DoubleValue(f)) }
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

        let data_point = NumberDataPoint {
            attributes,
            time_unix_nano,
            value: Some(self.value.as_proto()),
            ..Default::default()
        };

        let data = match self.data {
            MetricData::Gauge => {
                metric::Data::Gauge(Gauge { data_points: vec![data_point] })
            }
            MetricData::Sum { temporality, monotonic } => {
                metric::Data::Sum(Sum {
                    data_points: vec![data_point],
                    aggregation_temporality: temporality.as_proto() as i32,
                    is_monotonic: monotonic,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetricUnit {
    KibibytesPerSecond,
    KilobytesPerSecond,
    KilobitsPerSecond,

    Kibibytes,
    Kilobytes,
    Kilobits,

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
            Self::KilobitsPerSecond => "/s",

            Self::Kibibytes => "KiB",
            Self::Kilobytes => "KB",
            Self::Kilobits => "Kbit",

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
