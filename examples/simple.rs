use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Debug)]
struct Error<'a> {
    source: Option<&'a (dyn std::error::Error + 'static)>,
}

impl<'a> std::fmt::Display for Error<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error blabla")
    }
}

impl<'a> std::error::Error for Error<'a> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
    }
}

#[tokio::main]
async fn main() {
    let api_key = std::env::var("AXIOM_API_KEY")
        .expect("AXIOM_API_KEY environment variable to be valid");

    let axiom: tracing_axiom::Axiom =
        tracing_axiom::init(tracing_axiom::Config {
            evt_que_len: 4 << 10,
            service_name: "example-service",
            base_url: "https://api.axiom.co".parse().unwrap(),
            api_key: &api_key,
            dataset: "porting_test",
            collect_target: 4 << 10,
            collect_timeout: std::time::Duration::from_millis(500),
        });

    use tracing_subscriber::filter;
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(filter::LevelFilter::INFO.into())
                .parse_lossy(std::env::var("RUST_LOG").unwrap_or_default()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_axiom::layer(axiom.evt_tx.clone()));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    async {
        tracing::info!(value = 42, "start");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        tracing::info!(value = 42, "end");
        let err = Error {
            source: Some(&Error { source: Some(&Error { source: None }) }),
        };
        tracing::error!(
            error = &err as &dyn std::error::Error,
            "something errorred"
        );
    }
    .instrument(tracing::info_span!("parent span"))
    .await;

    // Try panicking to see the last-ditch effort Drop impl take effect!
    //
    // panic!("test");
    // std::thread::spawn(|| panic!("test"));

    // Try commenting this line and be punished for letting axiom be
    // dropped >:D
    //
    axiom.deinit().await;
}
