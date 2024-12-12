#!/bin/bash

SRC_DIR="./src/main/java"
BUILD_DIR="build/classes"
JAR_NAME="wordcount.jar"
MAIN_CLASS="org.example.beam.batch.WordCount"
INPUT_DIR="../data/input_files"
OUTPUT_DIR="output_dir"
RESULTS_DIR="baseline_results"
NUM_RUNS=3

echo "Creating directories..."
mkdir -p ${BUILD_DIR} ${RESULTS_DIR}

echo "Compiling Java source files..."
javac -d ${BUILD_DIR} -cp $(hadoop classpath) ${SRC_DIR}/org/example/beam/batch/WordCount.java

echo "Packaging into JAR file..."
jar cf ${JAR_NAME} -C ${BUILD_DIR} .

collect_metrics() {
    local pid=$1
    local interval=0.5
    local peak_memory=0
    local cpu_total=0
    local max_cpu=0
    local sample_count=0

    while kill -0 ${pid} 2> /dev/null; do
        mem_usage=$(ps -o rss= -p ${pid} | awk '{print $1/1024}')
        cpu_usage=$(ps -o %cpu= -p ${pid} | awk '{print $1}')
        
        if [[ ! -z "$mem_usage" && ! -z "$cpu_usage" ]]; then
            if (( $(echo "$mem_usage > $peak_memory" | bc -l) )); then
                peak_memory=$mem_usage
            fi
            
            cpu_total=$(echo "$cpu_total + $cpu_usage" | bc -l)
            if (( $(echo "$cpu_usage > $max_cpu" | bc -l) )); then
                max_cpu=$cpu_usage
            fi
            
            sample_count=$((sample_count + 1))
        fi
        
        sleep ${interval}
    done

    if [ $sample_count -gt 0 ]; then
        avg_cpu=$(echo "scale=2; $cpu_total / $sample_count" | bc -l)
    else
        avg_cpu=0
    fi

    echo "$peak_memory $avg_cpu $max_cpu"
}

input_files=(${INPUT_DIR}/*.txt)

for input_file in "${input_files[@]}"; do
    filename=$(basename "$input_file")
    basename="${filename%.*}"
    metrics_file="${RESULTS_DIR}/baseline_${basename}_results.csv"
    
    echo "Processing file: $filename"
    echo "Results will be saved to: $metrics_file"
    
    echo "Run Number,Execution Time (ms),Peak Memory (MB),Avg CPU (%),Max CPU (%),Throughput (words/sec),Latency (ms/word),Input Size" > "${metrics_file}"
    
    for run in $(seq 1 ${NUM_RUNS}); do
        echo "Running MapReduce job - File: $filename, Run ${run}..."

        current_output_dir="${OUTPUT_DIR}_${basename}_${run}"

        start_time=$(date +%s.%N)

        hadoop jar ${JAR_NAME} ${MAIN_CLASS} "$input_file" "$current_output_dir" &
        job_pid=$!
        
        metrics=$(collect_metrics ${job_pid})
        read peak_memory avg_cpu max_cpu <<< ${metrics}

        wait ${job_pid}

        end_time=$(date +%s.%N)
        execution_time=$(echo "($end_time - $start_time) * 1000" | bc | cut -d'.' -f1)

        input_size=$(wc -l < "$input_file")
        total_records_processed=$((input_size * 3))
        
        if [ $execution_time -gt 0 ]; then
            throughput=$(echo "scale=2; $total_records_processed / ($execution_time / 1000)" | bc)
            latency=$(echo "scale=5; $execution_time / $total_records_processed" | bc)
        else
            throughput=0
            latency=0
        fi

        peak_memory=$(printf "%.2f" $peak_memory)
        avg_cpu=$(printf "%.2f" $avg_cpu)
        max_cpu=$(printf "%.2f" $max_cpu)

        echo "${run},${execution_time},${peak_memory},${avg_cpu},${max_cpu},${throughput},${latency},${input_size}" >> "${metrics_file}"

        echo "Run ${run} Complete:"
        echo "  Execution Time: ${execution_time}ms"
        echo "  Peak Memory: ${peak_memory}MB"
        echo "  Avg CPU: ${avg_cpu}%"
        echo "  Max CPU: ${max_cpu}%"
        echo "  Input Size: ${input_size} lines"
        echo "  Throughput: ${throughput} words/sec"
        echo "  Latency: ${latency} ms/word"
        echo "----------------------------------------"
    done
done

echo "All files processed. Results saved to ${RESULTS_DIR}/ directory."