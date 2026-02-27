use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Holds OTel shutdown guards. Drop to flush.
pub struct Telemetry {
    _tracer_provider: SdkTracerProvider,
    _meter_provider: SdkMeterProvider,
}

pub struct Metrics {
    pub cdc_events_processed: Counter<u64>,
    pub cdc_events_failed: Counter<u64>,
    pub cdc_batch_duration: Histogram<f64>,
    pub backfill_rows_processed: Counter<u64>,
    pub backfill_batches_failed: Counter<u64>,
    pub backfill_batch_duration: Histogram<f64>,
    pub dlq_size: Gauge<u64>,
    pub dlq_replayed: Counter<u64>,
    pub dlq_replay_failed: Counter<u64>,
    pub turbopuffer_requests: Counter<u64>,
    pub turbopuffer_latency: Histogram<f64>,
    pub replication_acks: Counter<u64>,
}

/// Sets up tracing + metrics export via OTLP. Keep the returned
/// `Telemetry` alive for the lifetime of the process.
pub fn init(otlp_endpoint: &str) -> Result<(Telemetry, Metrics), crate::CliError> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(false);

    let span_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(otlp_endpoint)
        .with_protocol(Protocol::HttpBinary)
        .build()
        .map_err(|e| crate::CliError::Run(format!("failed to create OTLP span exporter: {e}")))?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .build();

    let tracer = tracer_provider.tracer("puffgres");
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(otlp_endpoint)
        .with_protocol(Protocol::HttpBinary)
        .build()
        .map_err(|e| crate::CliError::Run(format!("failed to create OTLP metric exporter: {e}")))?;

    let metric_reader = PeriodicReader::builder(metric_exporter).build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(metric_reader)
        .build();

    let meter = meter_provider.meter("puffgres");
    let metrics = build_metrics(&meter);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_trace_layer)
        .init();

    Ok((
        Telemetry {
            _tracer_provider: tracer_provider,
            _meter_provider: meter_provider,
        },
        metrics,
    ))
}

/// Sets up a plain fmt subscriber for CLI output when no OTLP endpoint
/// is configured. Ensures tracing::info! etc. still appear on stderr.
pub fn init_fmt_only() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(false),
        )
        .init();
}

/// Install panic hook that emits tracing::error! so panics
/// flow through the OTel pipeline.
pub fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(|s| s.as_str()))
            .unwrap_or("unknown panic");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_default();
        tracing::error!(
            panic.payload = payload,
            panic.location = location,
            "panic occurred"
        );
        default_hook(info);
    }));
}

fn build_metrics(meter: &Meter) -> Metrics {
    Metrics {
        cdc_events_processed: meter
            .u64_counter("puffgres.cdc.events_processed")
            .with_description("Total CDC events processed")
            .build(),
        cdc_events_failed: meter
            .u64_counter("puffgres.cdc.events_failed")
            .with_description("CDC events sent to DLQ")
            .build(),
        cdc_batch_duration: meter
            .f64_histogram("puffgres.cdc.batch_duration_ms")
            .with_description("Time to process one CDC batch")
            .build(),
        backfill_rows_processed: meter
            .u64_counter("puffgres.backfill.rows_processed")
            .with_description("Backfill rows processed")
            .build(),
        backfill_batches_failed: meter
            .u64_counter("puffgres.backfill.batches_failed")
            .with_description("Backfill batch failures")
            .build(),
        backfill_batch_duration: meter
            .f64_histogram("puffgres.backfill.duration_ms")
            .with_description("Time per backfill batch")
            .build(),
        dlq_size: meter
            .u64_gauge("puffgres.dlq.size")
            .with_description("Current DLQ entry count")
            .build(),
        dlq_replayed: meter
            .u64_counter("puffgres.dlq.replayed")
            .with_description("DLQ entries successfully replayed")
            .build(),
        dlq_replay_failed: meter
            .u64_counter("puffgres.dlq.replay_failed")
            .with_description("DLQ replay attempts that failed")
            .build(),
        turbopuffer_requests: meter
            .u64_counter("puffgres.turbopuffer.requests")
            .with_description("Turbopuffer API calls")
            .build(),
        turbopuffer_latency: meter
            .f64_histogram("puffgres.turbopuffer.latency_ms")
            .with_description("Turbopuffer API latency")
            .build(),
        replication_acks: meter
            .u64_counter("puffgres.replication.acks")
            .with_description("Replication slot acks sent")
            .build(),
    }
}
