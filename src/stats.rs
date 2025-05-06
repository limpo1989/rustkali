use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use hdrhistogram::Histogram;

pub struct Stats {
    pub total_connections: AtomicU64,
    pub success_connections: AtomicU64,
    pub total_requests: AtomicU64,
    pub total_bytes_sent: AtomicU64,
    pub total_bytes_received: AtomicU64,
    pub latency_histogram: parking_lot::Mutex<Histogram<u64>>,
    pub is_warmup: parking_lot::Mutex<bool>,
    pub is_shutting_down: AtomicBool,
    pub last_print_time: parking_lot::Mutex<Instant>,
    pub last_print_count: parking_lot::Mutex<u64>,
    pub connection_errors: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
            .expect("Failed to create histogram");

        Self {
            total_connections: AtomicU64::new(0),
            success_connections: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            total_bytes_sent: AtomicU64::new(0),
            total_bytes_received: AtomicU64::new(0),
            latency_histogram: parking_lot::Mutex::new(hist),
            is_warmup: parking_lot::Mutex::new(true),
            is_shutting_down: AtomicBool::new(false),
            last_print_time: parking_lot::Mutex::new(Instant::now()),
            last_print_count: parking_lot::Mutex::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }

    pub fn record_latency(&self, latency_us: u64) {
        if !*self.is_warmup.lock() {
            let mut hist = self.latency_histogram.lock();
            hist.record(latency_us).unwrap_or_else(|e| {
                if !self.is_shutting_down() {
                    eprintln!("Failed to record latency: {}", e);
                }
            });
        }
    }

    pub fn record_request(&self, bytes_sent: usize, bytes_received: usize) {
        if !*self.is_warmup.lock() {
            self.total_requests.fetch_add(1, Ordering::Relaxed);
            self.total_bytes_sent
                .fetch_add(bytes_sent as u64, Ordering::Relaxed);
            self.total_bytes_received
                .fetch_add(bytes_received as u64, Ordering::Relaxed);
        }
    }

    pub fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn end_warmup(&self) {
        *self.is_warmup.lock() = false;
        self.total_requests.store(0, Ordering::Relaxed);
        self.total_bytes_sent.store(0, Ordering::Relaxed);
        self.total_bytes_received.store(0, Ordering::Relaxed);
        self.latency_histogram.lock().reset();
        *self.last_print_time.lock() = Instant::now();
        *self.last_print_count.lock() = 0;
    }

    pub fn set_shutting_down(&self) {
        self.is_shutting_down.store(true, Ordering::Relaxed);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Relaxed)
    }

    pub fn get_qps(&self) -> f64 {
        let now = Instant::now();
        let mut last_time = self.last_print_time.lock();
        let mut last_count = self.last_print_count.lock();

        let elapsed = now.duration_since(*last_time).as_secs_f64();
        let current_count = self.total_requests.load(Ordering::Relaxed);
        let qps = (current_count - *last_count) as f64 / elapsed;

        *last_time = now;
        *last_count = current_count;

        qps
    }
}
