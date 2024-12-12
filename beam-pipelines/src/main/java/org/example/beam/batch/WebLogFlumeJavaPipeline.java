package org.example.beam.batch;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class WebLogFlumeJavaPipeline {
    public static void main(String[] args){
        if (args.length < 2) {
            System.err.println("Usage: WebLogFlumeJavaPipeline <inputFile1> <inputFile2> ...");
            System.exit(1);
        }

        Pipeline pipeline = Pipeline.create();

        List<String> inputFiles = Arrays.asList(args);

        PCollectionList<String> logCollections = PCollectionList.empty(pipeline);
        for (int i = 0; i < inputFiles.size(); i++) {
            String file = inputFiles.get(i);
            PCollection<String> logData = pipeline.apply("ReadLogs" + (i + 1), TextIO.read().from(file));
            logCollections = logCollections.and(logData);
        }

        PCollection<String> combinedLogData = logCollections.apply("FlattenLogs", Flatten.pCollections());

        PCollection<KV<String, String>> mappedLogs = combinedLogData.apply(ParDo.of(
            new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String logEntry = c.element();
                    String[] parts = logEntry.split(",");
                    if (parts.length >= 3) {
                        c.output(KV.of(parts[4], logEntry));
                    }
                }
            }
        ));

        PCollection<KV<String, Iterable<String>>> groupedLogs = 
            mappedLogs.apply(GroupByKey.create());

        PCollection<String> outputLogs = groupedLogs.apply(ParDo.of(
            new DoFn<KV<String, Iterable<String>>, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String timestamp = c.element().getKey();
                    Iterable<String> logs = c.element().getValue();
                    
                    int logCount = 0;
                    for (String log : logs) {
                        logCount++;
                    }
                    
                    System.out.println(timestamp + ": " + logCount + " logs");
                }
            }
        ));

        pipeline.run().waitUntilFinish();
    }
}