use std::collections::HashMap;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, Protocol, SpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Holds OTel shutdown guards. Call `shutdown()` before dropping to ensure
/// all pending telemetry is flushed to the backend.
pub struct Telemetry {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
}

impl Telemetry {
    /// Explicitly flush and shut down all providers. Call this before the
    /// process exits to ensure buffered telemetry reaches the backend.
    pub fn shutdown(&self) {
        if let Err(e) = self.logger_provider.shutdown() {
            eprintln!("otel: logger shutdown error: {e}");
        }
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("otel: tracer shutdown error: {e}");
        }
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("otel: meter shutdown error: {e}");
        }
    }
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

/// Sets up console-only tracing to stdout (no OTLP export).
pub fn init_console() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_writer(std::io::stdout);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

/// Sets up tracing to stdout + metrics/traces export via OTLP.
/// Keep the returned `Telemetry` alive for the lifetime of the process.
///
/// `otlp_endpoint` is the base OTLP endpoint (e.g. `https://host/otlp`).
/// Per-signal paths (`/v1/traces`, `/v1/metrics`) are appended here because
/// `.with_endpoint()` uses the URL verbatim — the library only appends
/// those paths when reading from the `OTEL_EXPORTER_OTLP_ENDPOINT` env var.
///
/// `otlp_headers` is an optional OTLP-formatted header string
/// (`key=value,key2=value2`) passed to each exporter via `.with_headers()`.
pub fn init(
    otlp_endpoint: &str,
    otlp_headers: Option<&str>,
) -> Result<(Telemetry, Metrics), crate::error::CliError> {
    let base = otlp_endpoint.trim_end_matches('/');
    let headers = parse_otlp_headers(otlp_headers.unwrap_or_default());

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", "puffgres")])
        .build();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_writer(std::io::stdout);

    let span_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/traces"))
        .with_protocol(Protocol::HttpJson)
        .with_headers(headers.clone())
        .build()
        .map_err(|e| crate::error::CliError::Otel(format!("span exporter: {e}")))?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    let tracer = tracer_provider.tracer("puffgres");
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/metrics"))
        .with_protocol(Protocol::HttpJson)
        .with_headers(headers.clone())
        .build()
        .map_err(|e| crate::error::CliError::Otel(format!("metric exporter: {e}")))?;

    let metric_reader = PeriodicReader::builder(metric_exporter).build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(metric_reader)
        .with_resource(resource.clone())
        .build();

    let meter = meter_provider.meter("puffgres");
    let metrics = build_metrics(&meter);

    let log_exporter = LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/logs"))
        .with_protocol(Protocol::HttpJson)
        .with_headers(headers)
        .build()
        .map_err(|e| crate::error::CliError::Otel(format!("log exporter: {e}")))?;

    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource)
        .build();

    let otel_log_layer = OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .init();

    Ok((
        Telemetry {
            tracer_provider,
            meter_provider,
            logger_provider,
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

/// Parse an OTLP-formatted header string (`key=value,key2=value2`) into a map.
/// Splits on the first `=` per entry so values may contain `=`.
fn parse_otlp_headers(raw: &str) -> HashMap<String, String> {
    raw.split(',')
        .filter_map(|pair| {
            let pair = pair.trim();
            let (k, v) = pair.split_once('=')?;
            Some((k.trim().to_string(), v.trim().to_string()))
        })
        .collect()
}
