package org.example.beam.batch;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class WebCountFlumeJavaBenchmark {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: WebCountFlumeJavaBenchmark <inputFile>");
            System.exit(1);
        }
        String inputFile = args[0];

        // create Pipeline
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("ReadLines", TextIO.read().from(inputFile))
            .apply("SplitIntoWords", ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String line, OutputReceiver<String> out) {
                    for (String word : line.split("\\s+")) {
                        if (!word.isEmpty()) {
                            out.output(word);
                        }
                    }
                }
            }))
            .apply("AssignCounts", ParDo.of(new DoFn<String, KV<String, Integer>>() {
                @ProcessElement
                public void processElement(@Element String word, OutputReceiver<KV<String, Integer>> out) {
                    out.output(KV.of(word, 1));
                }
            }))
            .apply("GroupByWord", GroupByKey.create())
            .apply("CountWords", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
                @ProcessElement
                public void processElement(@Element KV<String, Iterable<Integer>> wordCounts, OutputReceiver<KV<String, Integer>> out) {
                    int sum = 0;
                    for (int count : wordCounts.getValue()) {
                        sum += count;
                    }
                    out.output(KV.of(wordCounts.getKey(), sum));
                }
            }))
            .apply("PrintResults", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
                @ProcessElement
                public void processElement(@Element KV<String, Integer> wordCount) {
                    System.out.println(wordCount.getKey() + ": " + wordCount.getValue());
                }
            }));

        System.out.println("Running FlumeJava-Compatible Benchmark...");
        pipeline.run().waitUntilFinish();
        System.out.println("Benchmark Complete!");
    }
}