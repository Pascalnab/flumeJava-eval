#!/bin/bash

SRC_DIR="./src/main/java"
BUILD_DIR="build/classes"
JAR_NAME="weblog-pipeline.jar"
MAIN_CLASS="org.example.beam.batch.WebLogMapReducePipeline"
INPUT_FILE_1="./src/main/resources/ApacheLog1.csv"
INPUT_FILE_2="./src/main/resources/ApacheLog2.csv"
OUTPUT_DIR="output_dir_weblog"
METRICS_CSV="weblog_mapreduce_metrics.csv"
NUM_RUNS=3

echo "Cleaning up previous build and output..."
rm -rf ${BUILD_DIR} ${JAR_NAME} ${OUTPUT_DIR} ${METRICS_CSV}

echo "Creating build directory..."
mkdir -p ${BUILD_DIR}

echo "Compiling Java source files..."
javac -d ${BUILD_DIR} -cp $(hadoop classpath) ${SRC_DIR}/org/example/beam/batch/WebLogMapReducePipeline.java

echo "Packaging into JAR file..."
jar cf ${JAR_NAME} -C ${BUILD_DIR} .

echo "Run Number,Execution Time (ms),Peak Memory (MB),Avg CPU (%),Max CPU (%),Throughput (records/sec),Latency (ms/record),Total Input Size" > ${METRICS_CSV}

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

for run in $(seq 1 ${NUM_RUNS}); do
    echo "Running WebLog MapReduce job - Run ${run}..."

    start_time=$(date +%s.%N)

    hadoop jar ${JAR_NAME} ${MAIN_CLASS} ${INPUT_FILE_1} ${INPUT_FILE_2} &
    job_pid=$!
    
    metrics=$(collect_metrics ${job_pid})
    read peak_memory avg_cpu max_cpu <<< ${metrics}

    wait ${job_pid}

    end_time=$(date +%s.%N)
    execution_time=$(echo "($end_time - $start_time) * 1000" | bc | cut -d'.' -f1)

    input_size_1=$(wc -l < ${INPUT_FILE_1})
    input_size_2=$(wc -l < ${INPUT_FILE_2})
    total_input_size=$((input_size_1 + input_size_2))
    
    if [ $execution_time -gt 0 ]; then
        throughput=$(echo "scale=2; $total_input_size / ($execution_time / 1000)" | bc)
        latency=$(echo "scale=5; $execution_time / $total_input_size" | bc)
    else
        throughput=0
        latency=0
    fi

    peak_memory=$(printf "%.2f" $peak_memory)
    avg_cpu=$(printf "%.2f" $avg_cpu)
    max_cpu=$(printf "%.2f" $max_cpu)

    echo "${run},${execution_time},${peak_memory},${avg_cpu},${max_cpu},${throughput},${latency},${total_input_size}" >> ${METRICS_CSV}

    echo "Run ${run} Complete:"
    echo "  Execution Time: ${execution_time}ms"
    echo "  Peak Memory: ${peak_memory}MB"
    echo "  Avg CPU: ${avg_cpu}%"
    echo "  Max CPU: ${max_cpu}%"
    echo "  Input Size: ${total_input_size} records"
    echo "  Throughput: ${throughput} records/sec"
    echo "  Latency: ${latency} ms/record"
done

echo "Metrics collection complete. Results saved to ${METRICS_CSV}."

echo "Job completed. Verifying logs (printed to stdout)..."
echo "Check Hadoop logs or terminal output for results."