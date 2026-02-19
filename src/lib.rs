use std::borrow::Cow;
use std::num::NonZeroU64;

pub mod layer;

pub use reqwest::Url;

pub struct Config<'a> {
    pub api_key: &'a str,
    pub base_url: reqwest::Url,
    pub dataset: &'a str,
    /// Event queue length. Will start dropping events once this is full
    pub evt_que_len: usize,
    pub service_name: &'static str,

    /// Try to collect this many events before sending to axiom
    pub collect_target: usize,
    /// If we didn't collect up to target after this duratiom, timeout and send
    /// what we have.
    pub collect_timeout: std::time::Duration,
}
pub struct Axiom<X = Never> {
    pub evt_tx: tokio::sync::mpsc::Sender<Event<X>>,
    bg_task: tokio::task::JoinHandle<()>,
}
impl<X> Drop for Axiom<X> {
    fn drop(&mut self) {
        self.bg_task.abort();
    }
}
pub fn init<X>(cfg: Config) -> Axiom<X>
where
    X: serde::Serialize + std::marker::Send + 'static,
{
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::channel(cfg.evt_que_len);

    // NOTE: too much effort to bubble error here. this is run once on app init
    //       so this is fine. spurious crashlooping is impossible as the
    //       parsing is deterministic and config shouldn't be dynamic
    let ingest_url = cfg
        .base_url
        .join("v1/datasets")
        .unwrap()
        .join(cfg.dataset)
        .unwrap()
        .join("ingest")
        .unwrap();
    let bearer = reqwest::header::HeaderValue::try_from(
        format!("Bearer {}", cfg.api_key), //.
    )
    .unwrap();
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .redirect(reqwest::redirect::Policy::custom(|a| {
            let status = a.status().as_u16();
            // the two redirect types that discard the body
            if status == 302 || status == 303 {
                let to = a.url().clone();
                return a.error(LossyRedirect { status, to });
            }
            // delegate to default impl
            reqwest::redirect::Policy::default().redirect(a)
        }))
        .build()
        .unwrap();

    let rt = tokio::runtime::Handle::current();
    let bg_task = rt.spawn(async move {
        use std::ops::ControlFlow::{Break, Continue};

        use bytes::BufMut as _;

        let mut zstd_ctx = zstd::zstd_safe::CCtx::try_create().unwrap();

        let mut body = bytes::BytesMut::with_capacity(2048);
        let mut evts_buf = Vec::with_capacity(cfg.collect_target);
        let mut evts_count = 0;
        loop {
            body.clear();
            let mut body_writer = body.writer();

            let mut encoder = zstd::Encoder::with_context(
                &mut body_writer, //.
                &mut zstd_ctx,
            );

            match tokio::time::timeout(cfg.collect_timeout, async {
                let mut rest = cfg.collect_target;
                while rest > 0 {
                    evts_buf.clear();
                    let read = evt_rx.recv_many(&mut evts_buf, rest).await;
                    assert_eq!(read, evts_buf.len());
                    if read == 0 {
                        // Channel is closed
                        return Break(());
                    }
                    rest -= read;
                    evts_count += read;

                    for evt in &evts_buf {
                        use std::io::Write as _;
                        // ND-json: newline delimited
                        serde_json::to_writer(&mut encoder, &EventWrapper {
                            service: EventService { name: cfg.service_name },
                            event: evt,
                        })
                        .unwrap();
                        encoder.write_all(b"\n").unwrap();
                    }
                }
                assert!(evts_buf.len() == cfg.collect_target);
                Continue(())
            })
            .await
            {
                // forward shutfown sentinel
                Ok(Break(())) => return,
                Ok(Continue(())) | Err(tokio::time::error::Elapsed { .. }) => {}
            };
            assert!(!evts_buf.is_empty());
            assert!(evts_buf.len() <= cfg.collect_target);

            encoder.finish().unwrap();
            body = body_writer.into_inner();
            let body_shared = body.freeze();

            let mut backoff = std::time::Duration::from_millis(500);
            let mut reached_max_retry = true;
            const MAX_RETRIES: u16 = 100;
            for i in 0..MAX_RETRIES {
                let res = client
                    .post(ingest_url.clone())
                    .header(reqwest::header::AUTHORIZATION, &bearer)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .header(reqwest::header::CONTENT_ENCODING, "zstd")
                    .body(body_shared.clone())
                    .send()
                    .await
                    // axiom returns 200 with an error summary. nothing interesting
                    // in body for other codes
                    .and_then(|resp| resp.error_for_status());
                match res {
                    Ok(resp) => {
                        let status_raw = resp.bytes().await.unwrap();
                        let status: IngestStatus =
                            serde_json::from_slice(&status_raw).unwrap();
                        if status.failed > 0 || !status.failures.is_empty() {
                            tracing::error!(
                                ?backoff,
                                attempt = i,
                                status=?status_raw,
                                evts_count,
                                "axiom reported ingest failures");
                        } else {
                            reached_max_retry = false;
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::error!(
                            ?backoff, //.
                            attempt = i,
                            ?err,
                            evts_count,
                            "axiom request failed"
                        );
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = backoff.mul_f32(1.5);
            }
            if reached_max_retry {
                tracing::error!(
                    max_retries = MAX_RETRIES,
                    evts_count,
                    "reached max retries for ingest batch. dropping events!"
                );
            }

            // cross our fingers and hope reqwest didn't keep any refs to body.
            body = body_shared.into();
        }
    });

    Axiom { evt_tx, bg_task }
}

pub fn layer<X: std::fmt::Debug>(
    evt_tx: tokio::sync::mpsc::Sender<Event<X>>,
) -> layer::Layer<X> {
    layer::Layer::<X> {
        // service_name,
        sender: evt_tx,
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
pub enum Event<Extra = Never> {
    // see https://axiom.co/docs/query-data/traces
    // for Axiom trace schema list of reserved field names:
    // - _time
    // - trace_id
    // - span_id
    // - parent_span_id
    // - name
    // - kind
    // - duration
    // - error
    // - events
    // - links
    // - service.name // seems not documented but needed for trace explorer
    // - level
    //
    // more optional otel cringe:
    // - status.code
    // - status.message
    // - attributes
    // - resource
    //
    // owr own extra stuff:
    // - module_path
    // - data
    // - annotation_type
    // - idle_ns
    // - busy_ns
    // - target
    Log {
        #[serde(rename = "_time", with = "time::serde::rfc3339")]
        time: time::OffsetDateTime,
        #[serde(skip_serializing_if = "Option::is_none")]
        trace_id: Option<std::num::NonZeroU128>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_span_id: Option<std::num::NonZeroU64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        module_path: Option<Cow<'static, str>>,
        // TODO: derive error bool from level
        // error: bool,
        target: Cow<'static, str>,
        level: Level,
        name: Cow<'static, str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<Cow<'static, str>>,
        /// This field is meant to be a map field in axiom
        data: layer::FieldCascade,
    },
    Span {
        #[serde(rename = "_time", with = "time::serde::rfc3339")]
        time: time::OffsetDateTime,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_span_id: Option<std::num::NonZeroU64>,
        #[serde(flatten, skip_serializing_if = "Option::is_none")]
        span_info: Option<SpanInfo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        module_path: Option<Cow<'static, str>>,
        // TODO: derive error bool from level
        // error: bool,
        target: Cow<'static, str>,
        level: Level,
        name: Cow<'static, str>,
        // We shouldn't need this but seems rust type inference and/or macro
        // expansion order needs a little help.
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "layer::ser_option_field_cascade"
        )]
        /// This field is meant to be a map field in axiom
        data: Option<std::sync::Arc<std::sync::Mutex<layer::FieldCascade>>>,
    },
    Extra(Extra),
}

#[derive(Debug, serde::Serialize)]
pub struct SpanInfo {
    pub span_id: std::num::NonZeroU64,
    pub trace_id: std::num::NonZeroU128,
    pub duration_ns: u64,
    pub idle_ns: u64,
    pub busy_ns: u64,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}
impl From<tracing::Level> for Level {
    fn from(value: tracing::Level) -> Self {
        match value {
            tracing::Level::TRACE => Level::Trace,
            tracing::Level::DEBUG => Level::Debug,
            tracing::Level::INFO => Level::Info,
            tracing::Level::WARN => Level::Warn,
            tracing::Level::ERROR => Level::Error,
        }
    }
}

// https://axiom.co/docs/restapi/endpoints/ingestIntoDataset
#[allow(dead_code)]
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct IngestStatus {
    failed: u64,
    ingested: u64,
    processed_bytes: u64,
    blocks_created: Option<u32>,
    failures: Vec<IngestFailure>,
    wal_length: Option<u32>,
}

#[allow(dead_code)]
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct IngestFailure {
    error: String,
    timestamp: String,
}

#[derive(Debug)]
pub struct LossyRedirect {
    status: u16,
    to: Url,
}

impl std::fmt::Display for LossyRedirect {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Following such a redirect drops the request body, and will likely
        // give an HTTP 200 response even though nobody ever looked at the POST
        // body.
        //
        // This can e.g. happen for login redirects when you post to a
        // login-protected URL.
        write!(
            f,
            "lossy HTTP {} redirect to {} would cut off our body",
            self.status, self.to
        )
    }
}

impl std::error::Error for LossyRedirect {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    // fields whose value is `null` seem to be ignored by Honeycomb, so no Null variant
    // arrays and objects are not supported
    Bool(bool),
    Number(serde_json::Number),
    String(Cow<'static, str>),
    Map(std::collections::HashMap<Cow<'static, str>, Value>),
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

macro_rules! from_integer {
    ($($ty:ident)*) => {
        $(
            impl From<$ty> for Value {
                fn from(n: $ty) -> Self {
                    Value::Number(n.into())
                }
            }
        )*
    };
}

from_integer! {
    i8 i16 i32 i64 isize
    u8 u16 u32 u64 usize
}

impl From<f32> for Value {
    /// Convert 32-bit floating point number to `Value::Number`, or
    /// `Value::String` if infinite or NaN.
    fn from(f: f32) -> Self {
        // serde_json making Number::from_f32 private has forced my hand
        f64::from(f).into()
    }
}

impl From<f64> for Value {
    /// Convert 64-bit floating point number to `Value::Number`, or
    /// `Value::String` if infinite or NaN.
    fn from(f: f64) -> Self {
        serde_json::Number::from_f64(f)
            .map(Self::Number)
            // this is a little slimy but good behavior for honeycomb specifically
            // there's not really much else since we don't have a Null variant
            .unwrap_or_else(|| Self::String(format!("{}", f).into()))
    }
}

impl From<String> for Value {
    fn from(f: String) -> Self {
        Value::String(Cow::Owned(f))
    }
}

impl From<&str> for Value {
    fn from(f: &str) -> Self {
        Value::String(Cow::Owned(f.to_owned()))
    }
}

// we sacrifice generality here to prevent the footgun of
//  Cow<'static, str>::Borrowed(&'static str)::into() silently
//  converting into Cow::Owned
impl From<Cow<'static, str>> for Value {
    fn from(f: Cow<'static, str>) -> Self {
        Value::String(f)
    }
}

impl From<serde_json::Number> for Value {
    /// Convert `Number` to `Value::Number`.
    ///
    /// # Examples
    ///
    /// ```
    /// use serde_json::{Number, Value};
    ///
    /// let n = Number::from(7);
    /// let x: Value = n.into();
    /// ```
    fn from(f: serde_json::Number) -> Self {
        Value::Number(f)
    }
}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match self {
            Self::Bool(b) => serializer.serialize_bool(*b),
            Self::Number(n) => n.serialize(serializer),
            Self::String(s) => serializer.serialize_str(s),
            Self::Map(m) => serializer.collect_map(m),
        }
    }
}

/// A 8-byte value which identifies a given span.
///
/// The id is valid if it contains at least one non-zero byte.
#[derive(Clone, PartialEq, Eq, Copy, Hash)]
#[repr(transparent)]
pub struct SpanId(NonZeroU64);

impl From<NonZeroU64> for SpanId {
    fn from(value: NonZeroU64) -> Self {
        SpanId(value)
    }
}

impl std::fmt::Debug for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:016x}", self.0))
    }
}

impl std::fmt::Display for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:016x}", self.0))
    }
}

impl std::fmt::LowerHex for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::LowerHex::fmt(&self.0, f)
    }
}

impl serde::Serialize for SpanId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(format!("{}", self).as_ref())
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, PartialEq)]
pub enum Never {}

#[derive(serde::Serialize)]
struct EventWrapper<'a, X> {
    // NOTE: Axiom wants this otel artifact for their tooling to work properly
    //       ideally we'd just not have this or have this as an unnested
    //       `service_name` field, but alas.
    service: EventService<'a>,
    #[serde(flatten)]
    event: &'a Event<X>,
}
#[derive(serde::Serialize)]
struct EventService<'a> {
    name: &'a str,
}
