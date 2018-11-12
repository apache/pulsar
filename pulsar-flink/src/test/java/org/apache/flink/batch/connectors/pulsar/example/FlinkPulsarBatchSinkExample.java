package org.apache.flink.batch.connectors.pulsar.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.batch.connectors.pulsar.PulsarOutputFormat;
import org.apache.flink.util.Collector;

/**
 * Implements a batch word-count program on Pulsar topic by writing Flink DataSet.
 */
public class FlinkPulsarBatchSinkExample {

    private static final String EINSTEIN_QUOTE = "Imagination is more important than knowledge. " +
            "Knowledge is limited. Imagination encircles the world.";

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String TOPIC_NAME = "my-flink-topic";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final OutputFormat pulsarOutputFormat =
                new PulsarOutputFormat(SERVICE_URL, TOPIC_NAME, wordWithCount -> wordWithCount.toString().getBytes());

        DataSet<String> textDS = env.fromElements(EINSTEIN_QUOTE);

        textDS.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] words = value.toLowerCase().split(" ");
                for(String word: words) {
                    out.collect(new WordWithCount(word.replace(".", ""), 1));
                }
            }
        })
        .filter(wordWithCount -> wordWithCount.word.length() > 4)
        .groupBy(new KeySelector<WordWithCount, String>() {
            @Override
            public String getKey(WordWithCount wordWithCount) throws Exception {
                return wordWithCount.word;
            }
        })
        .reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount wordWithCount1, WordWithCount wordWithCount2) throws Exception {
                return  new WordWithCount(wordWithCount1.word, wordWithCount1.count + wordWithCount2.count);
            }
        })
        .output(pulsarOutputFormat);

        env.execute("Flink - Pulsar Batch WordCount");

    }

    /**
     * Data type for words with count.
     */
    private static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount { word = " + word + ", count = " + count + " }";
        }
    }
}