# tracing-axiom

[Axiom.co](axiom.co) backend for the tracing crate.

## Usage

Assumptions:
- `tokio` async runtime.
- `data` field configured as a mapped field in axiom dataset.

```rs
let axiom: tracing_axiom::Axiom =
    tracing_axiom::init(tracing_axiom::Config {
        evt_que_len: 4 << 10,
        service_name: "example-service",
        base_url: "https://api.axiom.co".parse().unwrap(),
        api_key: &api_key,
        dataset: "example-dataset",
        collect_target: 4 << 10,
        collect_timeout: std::time::Duration::from_millis(500),
        sender_pool_size: 1,
    });

// NOTE: can clone `axiom.evt_tx` and send custom events to it as long as they
//       implement `serde::Serialize`.

let subscriber = tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(tracing_axiom::layer(axiom.evt_tx.clone().downgrade()));
tracing::subscriber::set_global_default(subscriber).unwrap();

// Don't forget to deinit! Drop will panic! (if not panicking already)
axiom.deinit().await;
```

See `examples/simple.rs` for a working example.
