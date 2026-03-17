//! # tracing-axiom
//!
//! [Axiom.co](axiom.co) backend for the tracing crate.
//!
//! ## Usage
//!
//! Assumptions:
//! - `tokio` async runtime.
//! - `data` field configured as a mapped field in axiom dataset.
//!
//! ```rs
//! let axiom: tracing_axiom::Axiom =
//!     tracing_axiom::init(tracing_axiom::Config {
//!         evt_que_len: 4 << 10,
//!         service_name: "example-service",
//!         base_url: "https://us-east-1.aws.edge.axiom.co".parse().unwrap(),
//!         api_key: &api_key,
//!         dataset_id: "example-dataset",
//!         collect_target: 4 << 10,
//!         collect_timeout: std::time::Duration::from_millis(500),
//!         sender_pool_size: 1,
//!     });
//!
//! // NOTE: can clone `axiom.evt_tx` and send custom events to it as long as they
//! //       implement `serde::Serialize`.
//!
//! let subscriber = tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer())
//!     .with(tracing_axiom::layer(axiom.evt_tx.clone().downgrade()));
//! tracing::subscriber::set_global_default(subscriber).unwrap();
//!
//! // Don't forget to deinit! Drop will panic!
//! axiom.deinit().await;
//! ```
//!
//! See `examples/simple.rs` for a working example.
//!

use std::borrow::Cow;

pub use reqwest::Url;
use tracing::instrument::WithSubscriber as _;

pub mod layer;

pub(crate) const INTERNAL_TARGET: &str = "tracing_axiom::internal";

pub struct Config<'a> {
    pub api_key: &'a str,
    pub base_url: reqwest::Url,
    pub dataset_id: &'a str,
    /// Event queue length. Will start dropping events once this is full
    pub evt_que_len: usize,
    pub service_name: &'static str,

    /// Try to collect this many events before sending to axiom
    pub collect_target: usize,
    /// If we didn't collect up to target after this duratiom, timeout and send
    /// what we have.
    pub collect_timeout: std::time::Duration,
    /// Max number of concurrent sender jobs.
    pub sender_pool_size: usize,
}
pub struct Axiom<X: Send = Never> {
    // NOTE: ORER MATTERS. this sender needs to be dropped before _bg_handle
    pub evt_tx: tokio::sync::mpsc::Sender<Event<X>>,
    bg_handle: Option<tokio::task::JoinHandle<()>>,
}

pub fn init<X>(cfg: Config) -> Axiom<X>
where
    X: serde::Serialize + std::marker::Send + 'static,
{
    if cfg.sender_pool_size == 0 {
        panic!("sender_pool_size must be > 0");
    }

    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::channel(cfg.evt_que_len);

    // NOTE: too much effort to bubble error here. this is run once on app init
    //       so this is fine. spurious crashlooping is impossible as the
    //       parsing is deterministic and config shouldn't be dynamic
    let ingest_url =
        cfg.base_url.join(&format!("v1/ingest/{}", cfg.dataset_id)).unwrap();
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
    let bg_task = async move {
        let mut slots = Vec::with_capacity(cfg.sender_pool_size);
        for _ in 0..cfg.sender_pool_size {
            slots.push(SenderSlot::default());
        }
        let slots: Box<[SenderSlot]> = slots.into_boxed_slice();
        let (idle_tx, mut idle_rx) =
            tokio::sync::mpsc::channel(cfg.sender_pool_size);

        let coord = coord_task(
            &mut evt_rx,
            &mut idle_rx,
            &slots,
            cfg.collect_target,
            cfg.collect_timeout,
            cfg.service_name,
        );
        let senders = slots
            .iter()
            .enumerate()
            .map(|(id, slot)| SenderFut {
                done: false,
                fut: sender_task(
                    id,
                    slot,
                    &idle_tx,
                    &client,
                    &ingest_url,
                    &bearer,
                ),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut senders = Box::into_pin(senders);
        let senders =
            std::future::poll_fn(|cx| poll_senders(senders.as_mut(), cx));

        let ((), ()) = tokio::join!(coord, senders);
    };
    // Carry the caller's current dispatch onto the spawned bg task so its
    // internal logs still reach the app's other tracing layers.
    let bg_handle = rt.spawn(bg_task.with_current_subscriber());

    Axiom { evt_tx: evt_tx.clone(), bg_handle: Some(bg_handle) }
}

impl<X: Send> Axiom<X> {
    /// Call this instead of dropping! Drop doesn't support async
    pub async fn deinit(self) {
        // Non-dropping destructure. We drop the fields in this fn ourselves.
        let (evt_tx, bg_handle) = unsafe {
            let this = std::mem::ManuallyDrop::new(self);
            let Axiom { evt_tx, bg_handle } = &*this;
            (std::ptr::read(evt_tx), std::ptr::read(bg_handle))
        };

        let senders = evt_tx.strong_count() - 1;
        if senders > 0 {
            tracing::warn!(
                senders,
                "deinit Axiom handle while event senders still exist!"
            );
        }
        // This should be the last strong sender and so close the channel.
        // The bg task will detect this for a graceful shutdown.
        drop(evt_tx);

        bg_handle.unwrap().await.unwrap();
    }
}

// RAII + async = nonsense
//
/// Please DO NOT rely on this drop handler. This is nasty nonsense in
/// case of real panics.
///
/// Might even be worth catching panics in the calling code if it makes
/// sense so that the caller can properly call deinit().
impl<X: Send> Drop for Axiom<X> {
    fn drop(&mut self) {
        // This is a last ditch effort to not drop events.
        let mut bogus =
            Self { evt_tx: tokio::sync::mpsc::channel(1).0, bg_handle: None };
        std::mem::swap(self, &mut bogus);
        let actual = bogus;

        // block_in_place depends on a tokio feature flag.
        std::thread::scope(move |s| {
            let rt = tokio::runtime::Handle::current();
            s.spawn(move || rt.block_on(actual.deinit()));
        });

        if !std::thread::panicking() {
            panic!("call Axiom::deinit() instead of dropping it!");
        }
    }
}

pub fn layer<X>(
    evt_tx: tokio::sync::mpsc::WeakSender<Event<X>>,
) -> layer::Layer<X> {
    layer::Layer::<X> { sender: evt_tx }
}

#[derive(serde::Serialize)]
#[serde(untagged)]
pub enum Event<Extra = Never> {
    // Order of fields is optimized for json human readability.
    //
    // For Axiom trace schema list of required field names for tracing
    // integration, see https://axiom.co/docs/query-data/traces
    Log {
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<Cow<'static, str>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        module_path: Option<Cow<'static, str>>,
        target: Cow<'static, str>,
        level: Level,
        name: Cow<'static, str>,
        /// This field is meant to be a map field in axiom
        data: layer::FieldCascade,
        #[serde(rename = "_time", with = "time::serde::rfc3339")]
        time: time::OffsetDateTime,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "ser_opt_hex"
        )]
        trace_id: Option<[u8; 16]>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "ser_opt_hex"
        )]
        parent_span_id: Option<[u8; 8]>,
        kind: BogusKind,
    },
    Span {
        #[serde(skip_serializing_if = "Option::is_none")]
        module_path: Option<Cow<'static, str>>,
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
        #[serde(rename = "_time", with = "time::serde::rfc3339")]
        time: time::OffsetDateTime,
        #[serde(flatten, skip_serializing_if = "Option::is_none")]
        span_info: Option<SpanInfo>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "ser_opt_hex"
        )]
        parent_span_id: Option<[u8; 8]>,
        kind: BogusKind,
    },
    Extra(Extra),
}

impl<Extra: serde::Serialize> std::fmt::Debug for Event<Extra> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct FmtIo<'a, 'b>(&'a mut std::fmt::Formatter<'b>);

        impl std::io::Write for FmtIo<'_, '_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let s =
                    std::str::from_utf8(buf).map_err(std::io::Error::other)?;
                self.0.write_str(s).map_err(std::io::Error::other)?;
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        serde_json::to_writer(FmtIo(f), self).map_err(|_| std::fmt::Error)
    }
}

/// Axiom absolutely wants this for trace/span recognition so here it is.
pub struct BogusKind;
impl serde::Serialize for BogusKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("server")
    }
}

#[derive(Debug, serde::Serialize)]
pub struct SpanInfo {
    #[serde(serialize_with = "ser_hex")]
    pub span_id: [u8; 8],
    #[serde(serialize_with = "ser_hex")]
    pub trace_id: [u8; 16],
    #[serde(rename = "duration")]
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

// https://axiom.co/docs/restapi/endpoints/ingestToDataset
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
    to: reqwest::Url,
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

#[derive(Default)]
struct SenderSlot {
    /// Set by the coordinator, taken by exactly one sender task.
    state: tokio::sync::Mutex<SenderSlotState>,
    /// Wake the sender task after publishing a blob or closure.
    ready: tokio::sync::Notify,
}

#[derive(Default)]
struct SenderSlotState {
    blob: Option<BatchBlob>,
    closed: bool,
}

struct BatchBlob {
    body: bytes::Bytes,
    evts_count: usize,
}

struct SenderFut<F> {
    done: bool,
    fut: F,
}

fn poll_senders<F>(
    mut senders: std::pin::Pin<&mut [SenderFut<F>]>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<()>
where
    F: std::future::Future<Output = ()>,
{
    let mut pending = false;

    // SAFETY:
    // `senders` is pinned once as a boxed slice and never moved again.
    // We never reorder or replace elems, and only poll each future in place.
    for sender in unsafe { senders.as_mut().get_unchecked_mut() }.iter_mut() {
        if sender.done {
            continue;
        }

        match unsafe { std::pin::Pin::new_unchecked(&mut sender.fut) }.poll(cx)
        {
            std::task::Poll::Ready(()) => sender.done = true,
            std::task::Poll::Pending => pending = true,
        }
    }

    if pending { std::task::Poll::Pending } else { std::task::Poll::Ready(()) }
}

async fn sender_task(
    id: usize,
    slot: &SenderSlot,
    idle_tx: &tokio::sync::mpsc::Sender<usize>,
    client: &reqwest::Client,
    ingest_url: &reqwest::Url,
    bearer: &reqwest::header::HeaderValue,
) {
    loop {
        if idle_tx.send(id).await.is_err() {
            return;
        }

        slot.ready.notified().await;

        let blob = {
            let mut state = slot.state.lock().await;
            match state.blob.take() {
                Some(blob) => blob,
                None if state.closed => return,
                None => unreachable!(),
            }
        };
        send_blob(client, ingest_url, bearer, blob).await;

        if slot.state.lock().await.closed {
            return;
        }
    }
}

async fn coord_task<X>(
    evt_rx: &mut tokio::sync::mpsc::Receiver<Event<X>>,
    idle_rx: &mut tokio::sync::mpsc::Receiver<usize>,
    slots: &[SenderSlot],
    collect_target: usize,
    collect_timeout: std::time::Duration,
    service_name: &'static str,
) where
    X: serde::Serialize + Send + 'static,
{
    use std::io::Write as _;
    use std::ops::ControlFlow::{Break, Continue};

    use bytes::BufMut as _;

    let mut zstd_ctx = zstd::zstd_safe::CCtx::try_create().unwrap();
    let mut body = bytes::BytesMut::with_capacity(2048);
    let mut evts_buf = Vec::with_capacity(collect_target);
    loop {
        let mut evts_count = 0;

        body.clear();
        let mut body_writer = body.writer();
        let mut encoder = zstd::Encoder::with_context(
            &mut body_writer, //.
            &mut zstd_ctx,
        );

        let mut rest = collect_target;
        while evts_count == 0 {
            match tokio::time::timeout(collect_timeout, async {
                while rest > 0 {
                    evts_buf.clear();
                    let read = evt_rx.recv_many(&mut evts_buf, rest).await;
                    assert_eq!(read, evts_buf.len());
                    if read == 0 {
                        if evts_count == 0 && evts_buf.is_empty() {
                            return Break(());
                        }
                        return Continue(());
                    }
                    rest -= read;
                    evts_count += read;
                    for evt in &evts_buf {
                        serde_json::to_writer(
                            &mut encoder,
                            &EventWrapper {
                                service: EventService { name: service_name },
                                event: evt,
                            },
                        )
                        .unwrap();
                        encoder.write_all(b"\n").unwrap();
                    }
                }
                Continue(())
            })
            .await
            {
                Ok(Break(())) => {
                    close_slots(slots).await;
                    return;
                }
                Ok(Continue(())) | Err(tokio::time::error::Elapsed { .. }) => {}
            };
        }
        assert!(evts_count > 0);
        assert!(evts_count <= collect_target);

        encoder.finish().unwrap();
        body = body_writer.into_inner();
        let blob = BatchBlob { body: body.split().freeze(), evts_count };

        let id = idle_rx
            .recv()
            .await
            .expect("sender tasks exited before coordinator");
        let slot = &slots[id];
        {
            let mut state = slot.state.lock().await;
            assert!(state.blob.is_none());
            assert!(!state.closed);
            state.blob = Some(blob);
        }
        slot.ready.notify_one();
    }
}

async fn close_slots(slots: &[SenderSlot]) {
    for slot in slots {
        {
            let mut state = slot.state.lock().await;
            state.closed = true;
        }
        slot.ready.notify_one();
    }
}

async fn send_blob(
    client: &reqwest::Client,
    ingest_url: &reqwest::Url,
    bearer: &reqwest::header::HeaderValue,
    blob: BatchBlob,
) {
    let mut backoff = std::time::Duration::from_millis(500);
    let mut reached_max_retry = true;
    const MAX_RETRIES: u16 = 100;
    for i in 0..MAX_RETRIES {
        let res = client
            .post(ingest_url.clone())
            .header(reqwest::header::AUTHORIZATION, bearer)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::CONTENT_ENCODING, "zstd")
            .body(blob.body.clone())
            .send()
            .await
            .and_then(|resp| resp.error_for_status());
        match res {
            Ok(resp) => {
                let status_raw = resp.bytes().await.unwrap();
                let status: IngestStatus =
                    serde_json::from_slice(&status_raw).unwrap();
                if status.failed > 0 || !status.failures.is_empty() {
                    tracing::error!(
                        target: INTERNAL_TARGET,
                        ?backoff,
                        attempt = i,
                        status=?status_raw,
                        evts_count = blob.evts_count,
                        "axiom reported ingest failures"
                    );
                } else {
                    reached_max_retry = false;
                    break;
                }
            }
            Err(err) => {
                tracing::error!(
                    target: INTERNAL_TARGET,
                    ?backoff,
                    attempt = i,
                    ?err,
                    evts_count = blob.evts_count,
                    "axiom request failed"
                );
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = backoff.mul_f32(1.5);
    }
    if reached_max_retry {
        tracing::error!(
            target: INTERNAL_TARGET,
            max_retries = MAX_RETRIES,
            evts_count = blob.evts_count,
            "reached max retries for ingest batch. dropping events!"
        );
    }
}

fn ser_hex<const N: usize, S>(
    bytes: &[u8; N],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    fn chr(v: u8) -> u8 {
        u8::try_from(char::from_digit(v.into(), 16).unwrap()).unwrap()
    }

    // max size in onur use case is trace id which is 16 bytes
    const BUF_LEN: usize = 32;
    assert!(N * 2 <= BUF_LEN);
    let mut buf = [0u8; BUF_LEN];

    for (i, b) in bytes.iter().enumerate() {
        buf[i * 2] = chr(b / 16);
        buf[i * 2 + 1] = chr(b % 16);
    }

    serializer.serialize_str(unsafe { str::from_utf8_unchecked(&buf) })
}
fn ser_opt_hex<const N: usize, S>(
    bytes: &Option<[u8; N]>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    match bytes {
        Some(bytes) => ser_hex(bytes, serializer),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use axum::{
        Router,
        body::to_bytes,
        extract::{Request, State},
        response::IntoResponse,
        routing::post,
    };
    use serde::{Deserialize, Serialize};

    use super::{Config, Event, init};

    #[derive(Clone, Copy)]
    struct ServerCfg {
        delay: Duration,
        fail_first_batch_once: bool,
    }

    #[derive(Clone, Debug)]
    struct ReqObs {
        /// Lowest event seq seen in this ingest request body.
        start_seq: u64,
        /// Event count in this ingest request body.
        evts: usize,
    }

    #[derive(Deserialize, Serialize)]
    struct TestEvt {
        seq: u64,
    }

    #[derive(Default)]
    struct ServerObs {
        /// Per-request attempt count for the retry test.
        attempts: HashMap<u64, usize>,
        /// Request log in arrival order.
        reqs: Vec<ReqObs>,
        /// Current concurrent requests inside the stub server.
        in_flight: usize,
        /// Peak concurrent requests inside the stub server.
        max_in_flight: usize,
    }

    #[derive(Clone)]
    struct StubServer {
        /// Static behavior knobs for this server instance.
        cfg: ServerCfg,
        /// Mutable observations shared with the tests.
        obs: Arc<Mutex<ServerObs>>,
    }

    struct RunObs {
        /// Completed request log in arrival order.
        reqs: Vec<ReqObs>,
        /// Peak concurrent requests observed by the stub server.
        max_in_flight: usize,
    }

    struct TestServer {
        /// Bound local addr of the stub ingest server.
        addr: SocketAddr,
        /// Shared server state used both by the handler and the test.
        stub: StubServer,
        /// Triggers graceful server shutdown after the test run.
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
        /// Join handle for the stub ingest server task.
        server: tokio::task::JoinHandle<()>,
    }

    fn decode_req(body: bytes::Bytes) -> ReqObs {
        let body =
            zstd::stream::decode_all(std::io::Cursor::new(body.as_ref()))
                .unwrap();

        let mut start_seq = None;
        let mut evts = 0usize;
        for line in body.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let evt: TestEvt = serde_json::from_slice(line).unwrap();
            start_seq.get_or_insert(evt.seq);
            evts += 1;
        }

        ReqObs { start_seq: start_seq.unwrap(), evts }
    }

    fn record_req(stub: &StubServer, req: &ReqObs) -> bool {
        let mut obs = stub.obs.lock().unwrap();
        obs.in_flight += 1;
        obs.max_in_flight = obs.max_in_flight.max(obs.in_flight);
        obs.reqs.push(req.clone());

        let attempt = obs.attempts.entry(req.start_seq).or_insert(0);
        let should_fail = stub.cfg.fail_first_batch_once
            && req.start_seq == 0
            && *attempt == 0;
        *attempt += 1;
        should_fail
    }

    fn ingest_ok(evts: usize) -> impl IntoResponse {
        axum::Json(serde_json::json!({
            "failed": 0,
            "ingested": evts,
            "processedBytes": 0,
            "failures": [],
            "blocksCreated": null,
            "walLength": null,
        }))
    }

    async fn ingest(
        State(stub): State<StubServer>,
        req: Request,
    ) -> impl IntoResponse {
        let req =
            decode_req(to_bytes(req.into_body(), usize::MAX).await.unwrap());
        let should_fail = record_req(&stub, &req);

        if stub.cfg.delay > Duration::ZERO {
            tokio::time::sleep(stub.cfg.delay).await;
        }

        stub.obs.lock().unwrap().in_flight -= 1;

        let status = if should_fail {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        } else {
            axum::http::StatusCode::OK
        };
        (status, ingest_ok(req.evts))
    }

    impl TestServer {
        async fn new(cfg: ServerCfg) -> Self {
            let stub = StubServer {
                cfg,
                obs: Arc::new(Mutex::new(ServerObs::default())),
            };
            let app = Router::new()
                .route("/v1/ingest/test", post(ingest))
                .with_state(stub.clone());
            let listener = tokio::net::TcpListener::bind(SocketAddr::from((
                [127, 0, 0, 1],
                0,
            )))
            .await
            .unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown_tx, shutdown_rx) =
                tokio::sync::oneshot::channel::<()>();
            let server = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .unwrap();
            });
            Self { addr, stub, shutdown_tx, server }
        }

        fn mk_axiom(
            &self,
            evt_que_len: usize,
            collect_target: usize,
            sender_pool_size: usize,
        ) -> super::Axiom<TestEvt> {
            init(Config {
                evt_que_len,
                service_name: "test-service",
                base_url: format!("http://{}", self.addr).parse().unwrap(),
                api_key: "test-key",
                dataset_id: "test",
                collect_target,
                collect_timeout: Duration::from_secs(30),
                sender_pool_size,
            })
        }

        async fn finish(self, axiom: super::Axiom<TestEvt>) -> RunObs {
            axiom.deinit().await;
            let _ = self.shutdown_tx.send(());
            self.server.await.unwrap();

            let obs = self.stub.obs.lock().unwrap();
            RunObs { reqs: obs.reqs.clone(), max_in_flight: obs.max_in_flight }
        }
    }

    async fn send_evts(axiom: &super::Axiom<TestEvt>, count: u64) {
        for seq in 0..count {
            axiom.evt_tx.send(Event::Extra(TestEvt { seq })).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_sender_pool_1_preserves_request_order() {
        let srv = TestServer::new(ServerCfg {
            delay: Duration::from_millis(50),
            fail_first_batch_once: false,
        })
        .await;
        let axiom = srv.mk_axiom(8, 2, 1);

        send_evts(&axiom, 4).await;
        let obs = srv.finish(axiom).await;

        assert_eq!(obs.reqs.len(), 2);
        assert_eq!(obs.reqs[0].start_seq, 0);
        assert_eq!(obs.reqs[1].start_seq, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ingest_sender_pool_gt_1_allows_concurrent_sends() {
        let srv = TestServer::new(ServerCfg {
            delay: Duration::from_millis(150),
            fail_first_batch_once: false,
        })
        .await;
        let axiom = srv.mk_axiom(8, 1, 2);

        send_evts(&axiom, 4).await;
        let obs = srv.finish(axiom).await;

        assert!(obs.max_in_flight >= 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ingest_full_sender_pool_pushes_backpressure_upstream() {
        let srv = TestServer::new(ServerCfg {
            delay: Duration::from_millis(250),
            fail_first_batch_once: false,
        })
        .await;
        let axiom = srv.mk_axiom(1, 1, 1);

        send_evts(&axiom, 3).await;

        {
            let evt_tx = axiom.evt_tx.clone();
            let send = evt_tx.send(Event::Extra(TestEvt { seq: 3 }));
            tokio::pin!(send);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut send)
                    .await
                    .is_err()
            );
        }

        let _ = srv.finish(axiom).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ingest_retry_holds_sender_slot() {
        let srv = TestServer::new(ServerCfg {
            delay: Duration::ZERO,
            fail_first_batch_once: true,
        })
        .await;
        let axiom = srv.mk_axiom(8, 1, 1);

        send_evts(&axiom, 2).await;
        let obs = srv.finish(axiom).await;

        assert_eq!(
            obs.reqs.iter().map(|req| req.start_seq).collect::<Vec<_>>(),
            vec![0, 0, 1]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_shutdown_flushes_partial_batch() {
        let srv = TestServer::new(ServerCfg {
            delay: Duration::ZERO,
            fail_first_batch_once: false,
        })
        .await;
        let axiom = srv.mk_axiom(8, 4, 1);

        send_evts(&axiom, 3).await;
        let obs = srv.finish(axiom).await;

        assert_eq!(obs.reqs.len(), 1);
        assert_eq!(obs.reqs[0].start_seq, 0);
        assert_eq!(obs.reqs[0].evts, 3);
    }
}
