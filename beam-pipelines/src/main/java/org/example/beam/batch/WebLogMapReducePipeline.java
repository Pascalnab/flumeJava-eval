package org.example.beam.batch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class WebLogMapReducePipeline {
    public static class LogMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            
            if (parts.length >= 5) {
                outputKey.set(parts[4]); // timestamp
                outputValue.set(value);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class LogReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int logCount = 0;
            
            for (Text val : values) {
                logCount++;
            }
            
            System.out.println(key.toString() + ": " + logCount + " logs");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WebLogHadoopProcessor <input1> <input2>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Web Log Processor");
        job.setJarByClass(WebLogMapReducePipeline.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputFormatClass(NullOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, LogMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, LogMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
