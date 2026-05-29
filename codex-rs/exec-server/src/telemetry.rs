use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use codex_otel::MetricsClient;
use tracing::warn;

const CONNECTIONS_ACTIVE_METRIC: &str = "exec_server.connections.active";
const CONNECTIONS_TOTAL_METRIC: &str = "exec_server.connections.total";
const REQUESTS_TOTAL_METRIC: &str = "exec_server.requests.total";
const REQUEST_DURATION_METRIC: &str = "exec_server.request.duration";
const PROCESSES_ACTIVE_METRIC: &str = "exec_server.processes.active";
const PROCESS_DURATION_METRIC: &str = "exec_server.process.duration";
const RELAY_RECONNECTS_METRIC: &str = "exec_server.relay.reconnects";

#[derive(Clone, Copy)]
pub(crate) enum ConnectionTransport {
    Relay,
    Stdio,
    WebSocket,
}

impl ConnectionTransport {
    fn metric_tag(self) -> &'static str {
        match self {
            Self::Relay => "relay",
            Self::Stdio => "stdio",
            Self::WebSocket => "websocket",
        }
    }
}

#[derive(Clone, Default)]
pub struct ExecServerTelemetry {
    inner: Option<Arc<ExecServerTelemetryInner>>,
}

struct ExecServerTelemetryInner {
    metrics: MetricsClient,
    relay_connections: AtomicI64,
    stdio_connections: AtomicI64,
    websocket_connections: AtomicI64,
    active_processes: AtomicI64,
}

pub(crate) struct ConnectionMetricGuard {
    telemetry: ExecServerTelemetry,
    transport: ConnectionTransport,
}

impl ExecServerTelemetry {
    pub fn new(metrics: Option<MetricsClient>) -> Self {
        Self {
            inner: metrics.map(|metrics| {
                Arc::new(ExecServerTelemetryInner {
                    metrics,
                    relay_connections: AtomicI64::new(0),
                    stdio_connections: AtomicI64::new(0),
                    websocket_connections: AtomicI64::new(0),
                    active_processes: AtomicI64::new(0),
                })
            }),
        }
    }

    pub(crate) fn connection_started(
        &self,
        transport: ConnectionTransport,
    ) -> ConnectionMetricGuard {
        self.with_inner(|inner| {
            let active = inner
                .connection_counter(transport)
                .fetch_add(1, Ordering::AcqRel)
                + 1;
            inner.gauge(
                CONNECTIONS_ACTIVE_METRIC,
                active,
                &[("transport", transport.metric_tag())],
            );
            inner.counter(
                CONNECTIONS_TOTAL_METRIC,
                &[
                    ("transport", transport.metric_tag()),
                    ("result", "accepted"),
                ],
            );
        });
        ConnectionMetricGuard {
            telemetry: self.clone(),
            transport,
        }
    }

    pub(crate) fn request_completed(
        &self,
        operation: &'static str,
        result: &'static str,
        duration: Duration,
    ) {
        self.with_inner(|inner| {
            let tags = [("operation", operation), ("result", result)];
            inner.counter(REQUESTS_TOTAL_METRIC, &tags);
            inner.duration(REQUEST_DURATION_METRIC, duration, &tags);
        });
    }

    pub(crate) fn process_started(&self) {
        self.with_inner(|inner| {
            let active = inner.active_processes.fetch_add(1, Ordering::AcqRel) + 1;
            inner.gauge(PROCESSES_ACTIVE_METRIC, active, &[]);
        });
    }

    pub(crate) fn process_finished(&self, result: &'static str, duration: Duration) {
        self.with_inner(|inner| {
            let active = inner.active_processes.fetch_sub(1, Ordering::AcqRel) - 1;
            inner.gauge(PROCESSES_ACTIVE_METRIC, active, &[]);
            inner.duration(PROCESS_DURATION_METRIC, duration, &[("result", result)]);
        });
    }

    pub(crate) fn relay_reconnect(&self, reason: &'static str) {
        self.with_inner(|inner| {
            inner.counter(RELAY_RECONNECTS_METRIC, &[("reason", reason)]);
        });
    }

    fn connection_finished(&self, transport: ConnectionTransport) {
        self.with_inner(|inner| {
            let active = inner
                .connection_counter(transport)
                .fetch_sub(1, Ordering::AcqRel)
                - 1;
            inner.gauge(
                CONNECTIONS_ACTIVE_METRIC,
                active,
                &[("transport", transport.metric_tag())],
            );
        });
    }

    fn with_inner(&self, emit: impl FnOnce(&ExecServerTelemetryInner)) {
        if let Some(inner) = &self.inner {
            emit(inner);
        }
    }
}

impl Drop for ConnectionMetricGuard {
    fn drop(&mut self) {
        self.telemetry.connection_finished(self.transport);
    }
}

impl ExecServerTelemetryInner {
    fn connection_counter(&self, transport: ConnectionTransport) -> &AtomicI64 {
        match transport {
            ConnectionTransport::Relay => &self.relay_connections,
            ConnectionTransport::Stdio => &self.stdio_connections,
            ConnectionTransport::WebSocket => &self.websocket_connections,
        }
    }

    fn counter(&self, name: &str, tags: &[(&str, &str)]) {
        if self.metrics.counter(name, /*inc*/ 1, tags).is_err() {
            warn!(metric = name, "failed to emit exec-server counter");
        }
    }

    fn duration(&self, name: &str, duration: Duration, tags: &[(&str, &str)]) {
        if self.metrics.record_duration(name, duration, tags).is_err() {
            warn!(metric = name, "failed to emit exec-server duration");
        }
    }

    fn gauge(&self, name: &str, value: i64, tags: &[(&str, &str)]) {
        if self.metrics.gauge(name, value, tags).is_err() {
            warn!(metric = name, "failed to emit exec-server gauge");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use codex_otel::MetricsConfig;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use opentelemetry_sdk::metrics::data::AggregatedMetrics;
    use opentelemetry_sdk::metrics::data::Metric;
    use opentelemetry_sdk::metrics::data::MetricData;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use pretty_assertions::assert_eq;

    use super::ConnectionTransport;
    use super::ExecServerTelemetry;

    #[test]
    fn emits_bounded_exec_server_metrics() {
        let exporter = InMemoryMetricExporter::default();
        let metrics = codex_otel::MetricsClient::new(MetricsConfig::in_memory(
            "test",
            "codex-exec-server",
            env!("CARGO_PKG_VERSION"),
            exporter.clone(),
        ))
        .expect("metrics");
        let telemetry = ExecServerTelemetry::new(Some(metrics.clone()));

        let connection = telemetry.connection_started(ConnectionTransport::WebSocket);
        telemetry.request_completed("process", "success", Duration::from_millis(12));
        telemetry.process_started();
        telemetry.process_finished("success", Duration::from_millis(34));
        telemetry.relay_reconnect("connect_failed");
        drop(connection);
        metrics.shutdown().expect("shutdown metrics");

        let metrics = latest_metrics(&exporter);
        assert_eq!(
            metric_points(&metrics, "exec_server.connections.total"),
            vec![(
                1.0,
                BTreeMap::from([
                    ("result".to_string(), "accepted".to_string()),
                    ("transport".to_string(), "websocket".to_string()),
                ]),
            )]
        );
        assert_eq!(
            metric_points(&metrics, "exec_server.connections.active"),
            vec![(
                0.0,
                BTreeMap::from([("transport".to_string(), "websocket".to_string())]),
            )]
        );
        assert_eq!(
            metric_points(&metrics, "exec_server.requests.total"),
            vec![(
                1.0,
                BTreeMap::from([
                    ("operation".to_string(), "process".to_string()),
                    ("result".to_string(), "success".to_string()),
                ]),
            )]
        );
        assert_eq!(
            metric_points(&metrics, "exec_server.processes.active"),
            vec![(0.0, BTreeMap::new())]
        );
        assert_eq!(
            metric_points(&metrics, "exec_server.relay.reconnects"),
            vec![(
                1.0,
                BTreeMap::from([("reason".to_string(), "connect_failed".to_string())]),
            )]
        );
        assert_eq!(histogram_count(&metrics, "exec_server.request.duration"), 1);
        assert_eq!(histogram_count(&metrics, "exec_server.process.duration"), 1);
    }

    fn latest_metrics(exporter: &InMemoryMetricExporter) -> ResourceMetrics {
        exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .into_iter()
            .last()
            .expect("metrics export")
    }

    fn find_metric<'a>(resource_metrics: &'a ResourceMetrics, name: &str) -> &'a Metric {
        resource_metrics
            .scope_metrics()
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .find(|metric| metric.name() == name)
            .unwrap_or_else(|| panic!("metric {name} missing"))
    }

    fn metric_points(
        resource_metrics: &ResourceMetrics,
        name: &str,
    ) -> Vec<(f64, BTreeMap<String, String>)> {
        match find_metric(resource_metrics, name).data() {
            AggregatedMetrics::I64(MetricData::Gauge(gauge)) => gauge
                .data_points()
                .map(|point| (point.value() as f64, attributes_to_map(point.attributes())))
                .collect(),
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(|point| (point.value() as f64, attributes_to_map(point.attributes())))
                .collect(),
            _ => panic!("unexpected metric data for {name}"),
        }
    }

    fn histogram_count(resource_metrics: &ResourceMetrics, name: &str) -> u64 {
        match find_metric(resource_metrics, name).data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(opentelemetry_sdk::metrics::data::HistogramDataPoint::count)
                .sum(),
            _ => panic!("unexpected histogram data for {name}"),
        }
    }

    fn attributes_to_map<'a>(
        attributes: impl Iterator<Item = &'a KeyValue>,
    ) -> BTreeMap<String, String> {
        attributes
            .map(|attribute| {
                (
                    attribute.key.as_str().to_string(),
                    attribute.value.as_str().to_string(),
                )
            })
            .collect()
    }
}
