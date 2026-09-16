use {
    crate::rpc::admin::TpuActivityTracker,
    prometheus::{
        IntGauge,
        core::{Collector, Desc},
        proto::MetricFamily,
    },
    std::{sync::Arc, time::Instant},
};

const NAME: &str = "jet_tpu_forwarding_stalled";
const HELP: &str = "Whether TPU forwarding meets the stall condition in the trailing 30-second activity window (1 stalled, 0 otherwise)";

pub(crate) struct ForwardingMetrics {
    tracker: Arc<TpuActivityTracker>,
    desc: Desc,
}

impl ForwardingMetrics {
    pub(crate) fn new(tracker: Arc<TpuActivityTracker>) -> Self {
        Self {
            tracker,
            desc: Desc::new(NAME.to_owned(), HELP.to_owned(), vec![], Default::default())
                .expect("valid forwarding metric"),
        }
    }

    fn collect_at(&self, now: Instant) -> Vec<MetricFamily> {
        let gauge = IntGauge::new(NAME, HELP).expect("valid forwarding metric");
        gauge.set(i64::from(self.tracker.is_stalled(now)));
        gauge.collect()
    }
}

impl Collector for ForwardingMetrics {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.collect_at(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::Duration};

    fn value(metrics: &ForwardingMetrics, now: Instant) -> f64 {
        let families = metrics.collect_at(now);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name(), NAME);
        assert_eq!(
            families[0].get_field_type(),
            prometheus::proto::MetricType::GAUGE
        );
        assert_eq!(families[0].get_metric().len(), 1);
        assert!(families[0].get_metric()[0].get_label().is_empty());
        families[0].get_metric()[0].get_gauge().value()
    }

    #[test]
    fn stall_gauge_preserves_threshold_startup_success_and_idle_expiry() {
        let tracker = Arc::new(TpuActivityTracker::default());
        let metrics = ForwardingMetrics::new(Arc::clone(&tracker));
        let start = Instant::now();
        assert_eq!(value(&metrics, start), 0.0);
        for tick in 0..59 {
            tracker
                .failed_activity
                .increment(start + Duration::from_millis(tick * 100));
        }
        let now = start + Duration::from_millis(5900);
        assert_eq!(value(&metrics, now), 0.0);
        tracker.failed_activity.increment(now);
        assert_eq!(value(&metrics, now), 1.0);
        assert_eq!(value(&metrics, start + Duration::from_secs(36)), 0.0);
        tracker.sent_activity.increment(now);
        assert_eq!(value(&metrics, now), 0.0);
    }

    #[test]
    fn one_failure_burst_does_not_stall() {
        let tracker = Arc::new(TpuActivityTracker::default());
        let metrics = ForwardingMetrics::new(Arc::clone(&tracker));
        let now = Instant::now();
        tracker.failed_activity.increment_by(now, 10000);
        assert_eq!(value(&metrics, now), 0.0);
        assert_eq!(value(&metrics, now + Duration::from_secs(30)), 0.0);
    }

    #[test]
    fn success_expiry_can_reveal_sustained_failures() {
        let tracker = Arc::new(TpuActivityTracker::default());
        let metrics = ForwardingMetrics::new(Arc::clone(&tracker));
        let start = Instant::now();
        tracker.sent_activity.increment(start);
        for tick in 200..260 {
            tracker
                .failed_activity
                .increment(start + Duration::from_millis(tick * 100));
        }
        assert_eq!(value(&metrics, start + Duration::from_secs(26)), 0.0);
        assert_eq!(value(&metrics, start + Duration::from_secs(31)), 1.0);
        assert_eq!(value(&metrics, start + Duration::from_secs(56)), 0.0);
    }

    #[test]
    fn registry_exports_one_unlabelled_binary_gauge() {
        let registry = prometheus::Registry::new();
        registry
            .register(Box::new(ForwardingMetrics::new(Arc::new(
                TpuActivityTracker::default(),
            ))))
            .unwrap();
        let text = prometheus::TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert!(text.contains("# TYPE jet_tpu_forwarding_stalled gauge\n"));
        assert!(text.contains("jet_tpu_forwarding_stalled 0\n"));
        assert_eq!(registry.gather().len(), 1);
    }
}
