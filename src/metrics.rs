use std::collections::BTreeMap;

#[allow(unused_imports)]
use crate::proto::opentelemetry::proto::{
    collector::metrics::v1::{
        ExportMetricsPartialSuccess, ExportMetricsServiceRequest,
        ExportMetricsServiceResponse,
    },
    common::v1::{
        any_value, AnyValue, InstrumentationScope, KeyValue, KeyValueList,
    },
    metrics::v1::{
        metric, number_data_point, AggregationTemporality as ProtoAggregationTemporality,
        Gauge, Metric as ProtoMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
        Sum,
    },
    resource::v1::Resource,
};

pub struct Metric {
    pub service_name: String,
    pub host: String,
    pub source: MetricSource,
    pub attrs: BTreeMap<String, AttrValue>,
    pub name: String,
    pub unit: MetricUnit,
    pub data: MetricData,
    pub value: MetricValue,
}

pub struct MetricSource(String);

pub enum MetricData {
    Gauge,
    Sum {
        temporality: AggregationTemporality,
        monotonic: bool,
    }
}

pub enum AttrValue {
    Str(String),
    Uz(u64),
    F(f64)
}
pub enum AggregationTemporality {
    Delta,
    Cumulative,
}

pub enum MetricValue {
    F64(f64),
    I64(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetricUnit {
    KibibytesPerSecond,
    KilobytesPerSecond,
    KilobitsPerSecond,

    Kibibytes,
    Kilobytes,
    Kilobits ,

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

impl MetricUnit {
    fn as_proto(self) {

    }
    
}
