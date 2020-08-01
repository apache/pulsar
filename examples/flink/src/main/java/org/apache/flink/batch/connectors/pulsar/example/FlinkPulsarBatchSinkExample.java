/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flink.batch.connectors.pulsar.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.pulsar.PulsarOutputFormat;
import org.apache.flink.util.Collector;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

/**
 * Implements a batch word-count program on Pulsar topic by writing Flink DataSet.
 */
public class FlinkPulsarBatchSinkExample {

    private static final String EINSTEIN_QUOTE = "Imagination is more important than knowledge. " +
            "Knowledge is limited. Imagination encircles the world.";

    public static void main(String[] args) throws Exception {

        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!");
            System.out.println("Usage: pulsar --service-url <pulsar-service-url> --topic <topic>");
            return;
        }

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        String serviceUrl = parameterTool.getRequired("service-url");
        String topic = parameterTool.getRequired("topic");

        System.out.println("Parameters:");
        System.out.println("\tServiceUrl:\t" + serviceUrl);
        System.out.println("\tTopic:\t" + topic);

        // create PulsarOutputFormat instance
        final OutputFormat pulsarOutputFormat =
                new PulsarOutputFormat(serviceUrl, topic, new AuthenticationDisabled(), wordWithCount -> wordWithCount.toString().getBytes());

        // create DataSet
        DataSet<String> textDS = env.fromElements(EINSTEIN_QUOTE);

        // convert sentences to words
        textDS.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] words = value.toLowerCase().split(" ");
                for(String word: words) {
                    out.collect(new WordWithCount(word.replace(".", ""), 1));
                }
            }
        })

        // filter words which length is bigger than 4
        .filter(wordWithCount -> wordWithCount.word.length() > 4)

        // group the words
        .groupBy(new KeySelector<WordWithCount, String>() {
            @Override
            public String getKey(WordWithCount wordWithCount) throws Exception {
                return wordWithCount.word;
            }
        })

        // sum the word counts
        .reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount wordWithCount1, WordWithCount wordWithCount2) throws Exception {
                return  new WordWithCount(wordWithCount1.word, wordWithCount1.count + wordWithCount2.count);
            }
        })

        // write batch data to Pulsar
        .output(pulsarOutputFormat);

        // set parallelism to write Pulsar in parallel (optional)
        env.setParallelism(2);

        // execute program
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