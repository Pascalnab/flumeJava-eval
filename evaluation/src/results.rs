use crate::metrics::BeamMetrics;
use anyhow::Result;
use plotters::prelude::*;

pub fn save_metrics_to_csv(metrics: &[BeamMetrics], filename: &str) -> Result<()> {
    let mut writer = csv::Writer::from_path(filename)?;

    writer.write_record(
        &[
            "Run Number",
            "Execution Time (ms)",
            "Peak Memory (MB)",
            "Avg CPU (%)",
            "Max CPU (%)",
            "Throughput (words/sec)",
            "Latency (ms/word)",
            "Input Size",
            "Pipeline Stages",
            "Total Records Processed",
        ]
    )?;

    let total_records_processed: usize = metrics.iter().map(|m| m.records_processed).sum();

    for (run_id, metric) in metrics.iter().enumerate() {
        writer.write_record(
            &[
                format!("{}", run_id + 1),
                format!("{}", metric.execution_time_ms),
                format!("{:.2}", metric.peak_memory_mb),
                format!("{:.2}", metric.avg_cpu_usage),
                format!("{:.2}", metric.max_cpu_usage),
                format!("{:.2}", metric.throughput),
                format!("{:.5}", metric.latency_per_record),
                format!("{}", metric.records_processed),
                format!("{}", metric.pipeline_stages),
                format!("{}", total_records_processed),
            ]
        )?;
    }

    writer.flush()?;
    Ok(())
}

pub fn generate_execution_graph(metrics: &[BeamMetrics], output_path: &str) -> Result<()> {
    use plotters::style::colors::{BLUE, WHITE};

    if metrics.is_empty() {
        eprintln!("No metrics provided. Skipping graph generation.");
        return Ok(());
    }

    let root = BitMapBackend::new(output_path, (800, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let max_time = metrics
        .iter()
        .map(|m| m.execution_time_ms)
        .max()
        .unwrap_or(1) as f32;

    let mut chart = ChartBuilder::on(&root)
        .caption("Execution Time per Run", ("sans-serif", 30))
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0..metrics.len(), 0f32..max_time * 1.2)?;

    chart
        .configure_mesh()
        .x_desc("Run Number")
        .y_desc("Execution Time (ms)")
        .axis_desc_style(("sans-serif", 15))
        .draw()?;

        chart.draw_series(
        metrics
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let x0 = i; 
                let x1 = i + 1; 
                let y1 = m.execution_time_ms as f32;

                Rectangle::new(
                    [
                        (x0, 0.0),
                        (x1, y1),
                    ],
                    BLUE.filled()
                )
            })
    )?;

    chart.draw_series(
        metrics
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let x_center = i + 1;
                let y_top = m.execution_time_ms as f32;

                Text::new(
                    format!("{:.0}", y_top),
                    (x_center, y_top + max_time * 0.05),
                    ("sans-serif", 12).into_font()
                )
            })
    )?;

    root.present()?;
    println!("Graph saved to {}", output_path);
    Ok(())
}