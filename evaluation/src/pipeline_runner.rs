use crate::metrics::{ BeamMetrics, SystemSample };
use anyhow::Result;
use sysinfo::{ System, SystemExt, CpuExt };
use std::{ fs::File, io::{ self, BufRead }, process::Command, thread, time::{ Duration, Instant } };

fn count_lines_in_file(file_path: &str) -> Result<usize> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    Ok(reader.lines().count())
}

pub fn run_beam_pipeline(
    input_files: &[&str],
    benchmark_type: &str,
    pipeline_stages: usize,
) -> Result<BeamMetrics> {
    let mut sys = System::new_all();
    let start = Instant::now();
    let mut samples = Vec::new();

    println!("Running pipeline with input files: {:?}", input_files);

    let records_processed: usize = input_files
        .iter()
        .map(|file| count_lines_in_file(file).unwrap_or(0))
        .sum();

    // base command
    let mut binding = Command::new("java");
    let command = binding.arg("-Xmx1g");

    // configure command
    match benchmark_type {
        "baseline" => {
            if input_files.len() != 1 {
                return Err(anyhow::anyhow!("Baseline benchmark requires exactly one input file"));
            }
            command
                .arg("-jar")
                .arg("../beam-pipelines/target/baseline-pipeline.jar")
                .arg(input_files[0]);
        }
        "weblog" => {
            if input_files.len() < 2 {
                return Err(anyhow::anyhow!("WebLog benchmark requires at least two input files"));
            }
            command.arg("-jar").arg("../beam-pipelines/target/web-log-pipeline.jar");

            for file in input_files {
                command.arg(file);
            }
        }
        _ => {
            return Err(anyhow::anyhow!("Invalid benchmark type"));
        }
    }

    println!("Executing command: {:?}", command);

    let sample_interval = 100;


    // spawn and monitor thread
    let mut child = command.spawn()?;

    // collect metrics while pipeline is running
    while child.try_wait()?.is_none() {
        sys.refresh_all();
        samples.push(SystemSample {
            timestamp_ms: start.elapsed().as_millis(),
            memory_mb: (sys.used_memory() as f64) / 1024.0 / 1024.0,
            cpu_usage: sys.global_cpu_info().cpu_usage(),
        });
        thread::sleep(Duration::from_millis(sample_interval));
    }

    // calculate metrics
    let duration = start.elapsed().as_millis();
    let peak_memory = samples
        .iter()
        .map(|s| s.memory_mb)
        .fold(0.0, f64::max);
    let avg_cpu = if samples.is_empty() {
        0.0
    } else {
        samples
            .iter()
            .map(|s| s.cpu_usage)
            .sum::<f32>() / (samples.len() as f32)
    };
    let max_cpu = samples
        .iter()
        .map(|s| s.cpu_usage)
        .fold(0.0, f32::max);

    let throughput = (records_processed as f64) / ((duration as f64) / 1000.0);
    let latency_per_record = (duration as f64) / (records_processed as f64);

    Ok(BeamMetrics {
        execution_time_ms: duration,
        peak_memory_mb: peak_memory,
        avg_cpu_usage: avg_cpu,
        max_cpu_usage: max_cpu,
        throughput,
        latency_per_record,
        samples,
        records_processed,
        pipeline_stages,
    })
}
