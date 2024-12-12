use serde::Serialize;

#[derive(Debug)]
pub struct BeamMetrics {
    pub execution_time_ms: u128,
    pub peak_memory_mb: f64,
    pub avg_cpu_usage: f32,
    pub max_cpu_usage: f32,
    pub throughput: f64,
    pub latency_per_record: f64,
    pub samples: Vec<SystemSample>,
    pub records_processed: usize,
    pub pipeline_stages: usize,
}

#[derive(Debug)]
pub struct SystemSample {
    pub timestamp_ms: u128,
    pub memory_mb: f64,
    pub cpu_usage: f32,
}