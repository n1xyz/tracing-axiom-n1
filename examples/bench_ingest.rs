use std::time::{Duration, Instant};

use clap::Parser as _;
use rand::SeedableRng as _;
use rand::seq::SliceRandom as _;
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(clap::Parser, Debug)]
#[command(name = "bench_ingest")]
struct Cli {
    /// Axiom ingest auth token. See https://axiom.co/docs/restapi/ingest
    #[arg(long)]
    api_key: String,
    /// Axiom edge deployment base URL. See https://axiom.co/docs/reference/regions
    #[arg(long, default_value = "https://us-east-1.aws.edge.axiom.co")]
    base_url: String,
    /// Dataset name to ingest into.
    #[arg(long)]
    dataset_id: String,
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = [1, 2, 4, 8]
    )]
    pool_sizes: Vec<usize>,
    #[arg(long, default_value_t = 256)]
    batches: usize,
    #[arg(long, default_value_t = 1024)]
    collect_target: usize,
    #[arg(long, default_value_t = 1)]
    evt_que_len: usize,
    #[arg(long, default_value_t = 250)]
    collect_timeout_ms: u64,
    #[arg(long, default_value_t = 128)]
    payload_bytes: usize,
}

fn main() {
    let cli = Cli::parse();

    assert!(!cli.pool_sizes.is_empty(), "--pool-sizes must not be empty");
    assert!(cli.batches > 0, "--batches must be > 0");
    assert!(cli.collect_target > 0, "--collect-target must be > 0");
    assert!(cli.evt_que_len > 0, "--evt-que-len must be > 0");
    assert!(
        cli.pool_sizes.iter().all(|&pool| pool > 0),
        "--pool-sizes values must be > 0",
    );

    let mut exec_order = cli.pool_sizes.clone();
    exec_order.shuffle(&mut rand::rngs::SmallRng::from_os_rng());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let payload: &'static str =
            Box::leak("x".repeat(cli.payload_bytes).into_boxed_str());
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(
                        tracing_subscriber::filter::LevelFilter::INFO.into(),
                    )
                    .parse_lossy(std::env::var("RUST_LOG").unwrap_or_default()),
            )
            .with(tracing_subscriber::fmt::layer());
        tracing::subscriber::set_global_default(subscriber).unwrap();

        eprintln!(
            concat!(
                "benching dataset={} base_url={} pools={:?} ",
                "exec_order={:?} batches={} collect_target={} ",
                "evt_que_len={} payload_bytes={}"
            ),
            cli.dataset_id,
            cli.base_url,
            cli.pool_sizes,
            exec_order,
            cli.batches,
            cli.collect_target,
            cli.evt_que_len,
            cli.payload_bytes,
        );

        let mut obs = Vec::with_capacity(exec_order.len());
        for (exec_idx, &pool_size) in exec_order.iter().enumerate() {
            eprintln!("phase {} pool_size={} ...", exec_idx + 1, pool_size,);
            obs.push(run_phase(&cli, payload, exec_idx, pool_size).await);
        }

        obs.sort_by_key(|phase| phase.pool_size);

        print_phase_table(&obs);
        println!();
        print_pct_table(&obs);
    });
}

async fn run_phase(
    opts: &Cli,
    payload: &'static str,
    exec_idx: usize,
    pool_size: usize,
) -> PhaseObs {
    let base_url: tracing_axiom::Url = opts.base_url.parse().unwrap();
    let axiom = tracing_axiom::init(tracing_axiom::Config {
        evt_que_len: opts.evt_que_len,
        met_que_len: opts.evt_que_len,
        service_name: "bench-ingest",
        base_url,
        api_key: &opts.api_key,
        datasets: tracing_axiom::DatasetIds::Events {
            dataset_id: &opts.dataset_id,
        },
        collect_target: opts.collect_target,
        collect_timeout: Duration::from_millis(opts.collect_timeout_ms),
        sender_pool_size: pool_size,
    });

    let mut enqueue_wait =
        Vec::with_capacity(opts.batches * opts.collect_target);
    let mut batch_enqueue = Vec::with_capacity(opts.batches);
    let push_t0 = Instant::now();

    for batch in 0..opts.batches {
        let batch_t0 = Instant::now();
        for evt in batch_evts(payload, exec_idx, batch, opts.collect_target) {
            let evt_t0 = Instant::now();
            axiom.evt_tx.send(evt).await.unwrap();
            enqueue_wait.push(nanos(evt_t0.elapsed()));
        }
        batch_enqueue.push(nanos(batch_t0.elapsed()));
    }

    let push = push_t0.elapsed();
    let drain_t0 = Instant::now();
    axiom.deinit().await;
    let drain = drain_t0.elapsed();

    PhaseObs {
        pool_size,
        batches: opts.batches,
        evts: opts.batches * opts.collect_target,
        push,
        drain,
        enqueue_wait,
        batch_enqueue,
    }
}

fn batch_evts(
    payload: &'static str,
    exec_idx: usize,
    batch: usize,
    collect_target: usize,
) -> impl Iterator<Item = tracing_axiom::Event<BenchEvt>> {
    (0..collect_target as u64).map(move |seq| {
        tracing_axiom::Event::Extra(BenchEvt {
            exec_idx,
            batch,
            seq,
            data: BenchData { payload },
        })
    })
}

fn print_phase_table(obs: &[PhaseObs]) {
    println!("throughput summary");
    println!(
        "{:<6} {:>7} {:>9} {:>8} {:>11} {:>8} {:>11} {:>9}",
        "pool",
        "batches",
        "events",
        "push s",
        "push ev/s",
        "flush s",
        "flush ev/s",
        "drain ms",
    );
    for phase in obs {
        let flush = phase.push + phase.drain;
        println!(
            "{:<6} {:>7} {:>9} {:>8.3} {:>11.0} {:>8.3} {:>11.0} {:>9.3}",
            phase.pool_size,
            phase.batches,
            phase.evts,
            phase.push.as_secs_f64(),
            phase.evts as f64 / phase.push.as_secs_f64(),
            flush.as_secs_f64(),
            phase.evts as f64 / flush.as_secs_f64(),
            phase.drain.as_secs_f64() * 1_000.0,
        );
    }
}

fn print_pct_table(obs: &[PhaseObs]) {
    println!("enqueue pressure percentiles (ms)");
    println!(
        "{:<6} {:<14} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "pool", "metric", "n", "p1", "p5", "p50", "p95", "p99",
    );
    for phase in obs {
        print_pct_row(phase.pool_size, "enqueue_wait", &phase.enqueue_wait);
        print_pct_row(phase.pool_size, "batch_enqueue", &phase.batch_enqueue);
    }
}

fn print_pct_row(pool_size: usize, metric: &str, samples: &[u64]) {
    println!(
        "{:<6} {:<14} {:>8} {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.3}",
        pool_size,
        metric,
        samples.len(),
        ns_to_ms(pct(samples, 1)),
        ns_to_ms(pct(samples, 5)),
        ns_to_ms(pct(samples, 50)),
        ns_to_ms(pct(samples, 95)),
        ns_to_ms(pct(samples, 99)),
    );
}

fn pct(samples: &[u64], pct: usize) -> u64 {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (pct * sorted.len()).div_ceil(100).max(1) - 1;
    sorted[rank]
}

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

fn nanos(dur: Duration) -> u64 {
    dur.as_nanos().try_into().unwrap()
}

#[derive(Debug)]
struct PhaseObs {
    pool_size: usize,
    batches: usize,
    evts: usize,
    push: Duration,
    drain: Duration,
    enqueue_wait: Vec<u64>,
    batch_enqueue: Vec<u64>,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
struct BenchEvt {
    exec_idx: usize,
    batch: usize,
    seq: u64,
    data: BenchData,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
struct BenchData {
    payload: &'static str,
}
