use std::sync::{Arc, OnceLock};

use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};

#[derive(Clone, Debug)]
pub struct MetricsRegistry {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    registry: Registry,
    operator_rows_in: CounterVec,
    operator_rows_out: CounterVec,
    operator_batches_in: CounterVec,
    operator_batches_out: CounterVec,
    operator_bytes_in: CounterVec,
    operator_bytes_out: CounterVec,
    operator_time_seconds: HistogramVec,
    shuffle_bytes_written: CounterVec,
    shuffle_bytes_read: CounterVec,
    shuffle_partitions_written: CounterVec,
    shuffle_partitions_read: CounterVec,
    shuffle_fetch_seconds: HistogramVec,
    spill_bytes: CounterVec,
    spill_time_seconds: HistogramVec,
    scheduler_queued_tasks: GaugeVec,
    scheduler_running_tasks: GaugeVec,
    scheduler_retries: CounterVec,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner::new()),
        }
    }

    pub fn record_operator(
        &self,
        query_id: &str,
        stage_id: u64,
        task_id: u64,
        operator: &str,
        rows_in: u64,
        rows_out: u64,
        batches_in: u64,
        batches_out: u64,
        bytes_in: u64,
        bytes_out: u64,
        secs: f64,
    ) {
        let labels = [
            query_id,
            &stage_id.to_string(),
            &task_id.to_string(),
            operator,
        ];
        self.inner
            .operator_rows_in
            .with_label_values(&labels)
            .inc_by(rows_in as f64);
        self.inner
            .operator_rows_out
            .with_label_values(&labels)
            .inc_by(rows_out as f64);
        self.inner
            .operator_batches_in
            .with_label_values(&labels)
            .inc_by(batches_in as f64);
        self.inner
            .operator_batches_out
            .with_label_values(&labels)
            .inc_by(batches_out as f64);
        self.inner
            .operator_bytes_in
            .with_label_values(&labels)
            .inc_by(bytes_in as f64);
        self.inner
            .operator_bytes_out
            .with_label_values(&labels)
            .inc_by(bytes_out as f64);
        self.inner
            .operator_time_seconds
            .with_label_values(&labels)
            .observe(secs.max(0.0));
    }

    pub fn record_shuffle_write(
        &self,
        query_id: &str,
        stage_id: u64,
        task_id: u64,
        bytes: u64,
        partitions: u64,
        secs: f64,
    ) {
        let labels = [query_id, &stage_id.to_string(), &task_id.to_string()];
        self.inner
            .shuffle_bytes_written
            .with_label_values(&labels)
            .inc_by(bytes as f64);
        self.inner
            .shuffle_partitions_written
            .with_label_values(&labels)
            .inc_by(partitions as f64);
        self.inner
            .shuffle_fetch_seconds
            .with_label_values(&labels)
            .observe(secs.max(0.0));
    }

    pub fn record_shuffle_read(
        &self,
        query_id: &str,
        stage_id: u64,
        task_id: u64,
        bytes: u64,
        partitions: u64,
        secs: f64,
    ) {
        let labels = [query_id, &stage_id.to_string(), &task_id.to_string()];
        self.inner
            .shuffle_bytes_read
            .with_label_values(&labels)
            .inc_by(bytes as f64);
        self.inner
            .shuffle_partitions_read
            .with_label_values(&labels)
            .inc_by(partitions as f64);
        self.inner
            .shuffle_fetch_seconds
            .with_label_values(&labels)
            .observe(secs.max(0.0));
    }

    pub fn record_spill(
        &self,
        query_id: &str,
        stage_id: u64,
        task_id: u64,
        kind: &str,
        bytes: u64,
        secs: f64,
    ) {
        let labels = [query_id, &stage_id.to_string(), &task_id.to_string(), kind];
        self.inner
            .spill_bytes
            .with_label_values(&labels)
            .inc_by(bytes as f64);
        self.inner
            .spill_time_seconds
            .with_label_values(&labels)
            .observe(secs.max(0.0));
    }

    pub fn set_scheduler_queued_tasks(&self, query_id: &str, stage_id: u64, queued: u64) {
        let labels = [query_id, &stage_id.to_string()];
        self.inner
            .scheduler_queued_tasks
            .with_label_values(&labels)
            .set(queued as f64);
    }

    pub fn set_scheduler_running_tasks(&self, query_id: &str, stage_id: u64, running: u64) {
        let labels = [query_id, &stage_id.to_string()];
        self.inner
            .scheduler_running_tasks
            .with_label_values(&labels)
            .set(running as f64);
    }

    pub fn inc_scheduler_retries(&self, query_id: &str, stage_id: u64) {
        let labels = [query_id, &stage_id.to_string()];
        self.inner
            .scheduler_retries
            .with_label_values(&labels)
            .inc();
    }

    pub fn render_prometheus(&self) -> String {
        let metric_families = self.inner.registry.gather();
        let mut out = Vec::new();
        let enc = TextEncoder::new();
        if enc.encode(&metric_families, &mut out).is_err() {
            return String::new();
        }
        String::from_utf8_lossy(&out).to_string()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsInner {
    fn new() -> Self {
        let registry = Registry::new();

        let operator_rows_in = counter_vec(
            &registry,
            "ffq_operator_rows_in_total",
            "Input rows processed per operator",
            &["query_id", "stage_id", "task_id", "operator"],
        );
        let operator_rows_out = counter_vec(
            &registry,
            "ffq_operator_rows_out_total",
            "Output rows produced per operator",
            &["query_id", "stage_id", "task_id", "operator"],
        );
        let operator_batches_in = counter_vec(
            &registry,
            "ffq_operator_batches_in_total",
            "Input batches processed per operator",
            &["query_id", "stage_id", "task_id", "operator"],
        );
        let operator_batches_out = counter_vec(
            &registry,
            "ffq_operator_batches_out_total",
            "Output batches produced per operator",
            &["query_id", "stage_id", "task_id", "operator"],
        );
        let operator_bytes_in = counter_vec(
            &registry,
            "ffq_operator_bytes_in_total",
            "Input bytes processed per operator",
            &["query_id", "stage_id", "task_id", "operator"],
        );
        let operator_bytes_out = counter_vec(
            &registry,
            "ffq_operator_bytes_out_total",
            "Output bytes produced per operator",
            &["query_id", "stage_id", "task_id", "operator"],
        );
        let operator_time_seconds = histogram_vec(
            &registry,
            "ffq_operator_time_seconds",
            "Time spent in each operator",
            &["query_id", "stage_id", "task_id", "operator"],
        );

        let shuffle_bytes_written = counter_vec(
            &registry,
            "ffq_shuffle_bytes_written_total",
            "Shuffle bytes written",
            &["query_id", "stage_id", "task_id"],
        );
        let shuffle_bytes_read = counter_vec(
            &registry,
            "ffq_shuffle_bytes_read_total",
            "Shuffle bytes read",
            &["query_id", "stage_id", "task_id"],
        );
        let shuffle_partitions_written = counter_vec(
            &registry,
            "ffq_shuffle_partitions_written_total",
            "Shuffle partitions written",
            &["query_id", "stage_id", "task_id"],
        );
        let shuffle_partitions_read = counter_vec(
            &registry,
            "ffq_shuffle_partitions_read_total",
            "Shuffle partitions read",
            &["query_id", "stage_id", "task_id"],
        );
        let shuffle_fetch_seconds = histogram_vec(
            &registry,
            "ffq_shuffle_fetch_seconds",
            "Shuffle fetch/write time",
            &["query_id", "stage_id", "task_id"],
        );

        let spill_bytes = counter_vec(
            &registry,
            "ffq_spill_bytes_total",
            "Spill bytes written",
            &["query_id", "stage_id", "task_id", "kind"],
        );
        let spill_time_seconds = histogram_vec(
            &registry,
            "ffq_spill_time_seconds",
            "Spill write time",
            &["query_id", "stage_id", "task_id", "kind"],
        );

        let scheduler_queued_tasks = gauge_vec(
            &registry,
            "ffq_scheduler_queued_tasks",
            "Currently queued tasks",
            &["query_id", "stage_id"],
        );
        let scheduler_running_tasks = gauge_vec(
            &registry,
            "ffq_scheduler_running_tasks",
            "Currently running tasks",
            &["query_id", "stage_id"],
        );
        let scheduler_retries = counter_vec(
            &registry,
            "ffq_scheduler_retries_total",
            "Task retries",
            &["query_id", "stage_id"],
        );

        Self {
            registry,
            operator_rows_in,
            operator_rows_out,
            operator_batches_in,
            operator_batches_out,
            operator_bytes_in,
            operator_bytes_out,
            operator_time_seconds,
            shuffle_bytes_written,
            shuffle_bytes_read,
            shuffle_partitions_written,
            shuffle_partitions_read,
            shuffle_fetch_seconds,
            spill_bytes,
            spill_time_seconds,
            scheduler_queued_tasks,
            scheduler_running_tasks,
            scheduler_retries,
        }
    }
}

fn counter_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> CounterVec {
    let c = CounterVec::new(Opts::new(name, help), labels).expect("counter vec");
    registry
        .register(Box::new(c.clone()))
        .expect("register counter");
    c
}

fn gauge_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(Opts::new(name, help), labels).expect("gauge vec");
    registry
        .register(Box::new(g.clone()))
        .expect("register gauge");
    g
}

fn histogram_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> HistogramVec {
    let h = HistogramVec::new(HistogramOpts::new(name, help), labels).expect("histogram vec");
    registry
        .register(Box::new(h.clone()))
        .expect("register histogram");
    h
}

static GLOBAL_METRICS: OnceLock<MetricsRegistry> = OnceLock::new();

pub fn global_metrics() -> &'static MetricsRegistry {
    GLOBAL_METRICS.get_or_init(MetricsRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::MetricsRegistry;

    #[test]
    fn renders_prometheus_text() {
        let m = MetricsRegistry::new();
        m.record_operator("q1", 0, 0, "ParquetScan", 0, 10, 0, 1, 0, 128, 0.01);
        let text = m.render_prometheus();
        assert!(text.contains("ffq_operator_rows_out_total"));
        assert!(text.contains("ParquetScan"));
    }

    #[test]
    fn renders_all_metric_families() {
        let m = MetricsRegistry::new();
        m.record_operator("q1", 1, 2, "HashJoin", 10, 4, 2, 1, 100, 80, 0.02);
        m.record_shuffle_write("q1", 1, 2, 1024, 4, 0.01);
        m.record_shuffle_read("q1", 2, 3, 2048, 4, 0.03);
        m.record_spill("q1", 2, 3, "aggregate", 512, 0.005);
        m.set_scheduler_queued_tasks("q1", 1, 3);
        m.set_scheduler_running_tasks("q1", 1, 2);
        m.inc_scheduler_retries("q1", 1);
        let text = m.render_prometheus();

        assert!(text.contains("ffq_operator_rows_in_total"));
        assert!(text.contains("ffq_operator_rows_out_total"));
        assert!(text.contains("ffq_operator_batches_in_total"));
        assert!(text.contains("ffq_operator_batches_out_total"));
        assert!(text.contains("ffq_operator_bytes_in_total"));
        assert!(text.contains("ffq_operator_bytes_out_total"));
        assert!(text.contains("ffq_operator_time_seconds"));

        assert!(text.contains("ffq_shuffle_bytes_written_total"));
        assert!(text.contains("ffq_shuffle_bytes_read_total"));
        assert!(text.contains("ffq_shuffle_partitions_written_total"));
        assert!(text.contains("ffq_shuffle_partitions_read_total"));
        assert!(text.contains("ffq_shuffle_fetch_seconds"));

        assert!(text.contains("ffq_spill_bytes_total"));
        assert!(text.contains("ffq_spill_time_seconds"));

        assert!(text.contains("ffq_scheduler_queued_tasks"));
        assert!(text.contains("ffq_scheduler_running_tasks"));
        assert!(text.contains("ffq_scheduler_retries_total"));
    }
}
