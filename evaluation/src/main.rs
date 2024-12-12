mod metrics;
mod pipeline_runner;
mod results;

use anyhow::Result;
use pipeline_runner::run_beam_pipeline;
use results::{ save_metrics_to_csv, generate_execution_graph };
use serde::Deserialize;
use std::env;
use std::fs;

#[derive(Debug, Deserialize)]
struct BenchmarkConfig {
    benchmark_type: String,
    iterations: usize,
    pipeline_stages: usize,
}

fn parse_args() -> BenchmarkConfig {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        eprintln!(
            "Usage: {} <benchmark_type> <iterations> <pipeline_stages>\n\n\
            Example:\n  {} baseline 3 1",
            args[0],
            args[0]
        );
        std::process::exit(1);
    }

    BenchmarkConfig {
        benchmark_type: args[1].clone(),
        iterations: args[2].parse().expect("Invalid iteration count"),
        pipeline_stages: args[3].parse().expect("Invalid pipeline stages"),
    }
}

// Main benchmarking function
fn main() -> Result<()> {
    let config = parse_args();
    println!("\n--- Starting Benchmark ---");
    println!("Configuration: {:?}", config);
 
    let input_files_dir = match config.benchmark_type.as_str() {
        "baseline" => "../data/input_files",
        "weblog" => "../beam-pipelines/src/main/resources",
        _ => {
            eprintln!("Invalid benchmark type: {}", config.benchmark_type);
            std::process::exit(1);
        }
    };
 
    let input_files = fs::read_dir(input_files_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|e| e.to_str()) == Some("txt"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
 
    if input_files.is_empty() {
        eprintln!("No input files found in {}", input_files_dir);
        std::process::exit(1);
    }
 
    // handle input files based on benchmark type
    let file_groups = match config.benchmark_type.as_str() {
        "baseline" => {
            input_files.iter().map(|f| vec![f.clone()]).collect::<Vec<_>>()
        }
        "weblog" => {
            vec![input_files]
        }
        _ => {
            eprintln!("Invalid benchmark type: {}", config.benchmark_type);
            std::process::exit(1);
        }
    };
 
    // run benchmarks for each file group
    for files in file_groups {
        let files_str: Vec<&str> = files.iter()
            .map(|p| p.to_str().unwrap())
            .collect();
 
        println!(
            "\nRunning Benchmark: '{}' with Input Files: {:?}",
            config.benchmark_type,
            files_str
        );
 
        let mut all_metrics = Vec::new();
 
        for iteration in 0..config.iterations {
            println!("  Iteration {}/{}", iteration + 1, config.iterations);
            let metrics = run_beam_pipeline(
                &files_str,
                &config.benchmark_type,
                config.pipeline_stages,
            )?;
 
            println!(
                "  Execution Time: {}ms | Peak Memory: {:.2}MB | Avg CPU: {:.2}%",
                metrics.execution_time_ms,
                metrics.peak_memory_mb,
                metrics.avg_cpu_usage
            );
            all_metrics.push(metrics);
        }
 
        // save metrics and generate graph
        let file_stem = if files.len() == 1 {
            files[0].file_stem().unwrap().to_str().unwrap().to_string()
        } else {
            "combined".to_string()
        };
        
        let csv_file = format!("{}_{}_results.csv", config.benchmark_type, file_stem);
        let graph_file = format!("{}_{}_execution_time.png", config.benchmark_type, file_stem);
 
        save_metrics_to_csv(&all_metrics, &csv_file)?;
        generate_execution_graph(&all_metrics, &graph_file)?;
 
        println!("Results saved: {} | Execution graph: {}\n", csv_file, graph_file);
    }
 
    println!("--- Benchmark Complete ---");
    Ok(())
 }