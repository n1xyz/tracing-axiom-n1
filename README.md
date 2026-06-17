# tracing-axiom

[Axiom.co](https://axiom.co) backend for the tracing crate, including wrapper around axiom ingests for events and metrics. 

Metrics ingest relies on vendoring opentelemetry protobuf files rather than us pulling in their rust crate as dependency.
These files can be sourced [here](https://github.com/open-telemetry/opentelemetry-proto/tree/main/opentelemetry/proto), are stored in `/proto`.

## Usage

Assumptions:
- `tokio` async runtime.
- `data` field configured as a map field in your Axiom dataset.
- `base_url` set to your org's Axiom edge deployment base domain:
  <https://axiom.co/docs/reference/regions>
- `api_key` set per Axiom ingest auth docs:
  <https://axiom.co/docs/restapi/ingest>

```rs
let axiom: tracing_axiom::Axiom =
    tracing_axiom::init(tracing_axiom::Config {
        evt_que_len: 4 << 10,
        met_que_len: 4 << 10, 
        service_name: "example-service", 
        base_url: "https://us-east-1.aws.edge.axiom.co".parse().unwrap(),
        api_key: &api_key,
        dataset_id: "example-dataset",
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

