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
package org.apache.flink.streaming.connectors.pulsar.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.avro.generated.WordWithCount;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.pulsar.PulsarAvroTableSink;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

/**
 * Implements a streaming wordcount program on pulsar topics.
 *
 * <p>Example usage:
 *   --service-url pulsar://localhost:6650 --input-topic test_topic --subscription test_sub
 * or
 *   --service-url pulsar://localhost:6650 --input-topic test_src --subscription test_sub --output-topic test_sub
 */
public class PulsarConsumerSourceWordCountToAvroTableSink {
    private static final String ROUTING_KEY = "word";

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 3) {
            System.out.println("Missing parameters!");
            System.out.println("Usage: pulsar --service-url <pulsar-service-url> --input-topic <topic> --subscription <sub> --output-topic <topic>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000);
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(env);

        String serviceUrl = parameterTool.getRequired("service-url");
        String inputTopic = parameterTool.getRequired("input-topic");
        String subscription = parameterTool.get("subscription", "flink-examples");
        String outputTopic = parameterTool.get("output-topic", null);
        int parallelism = parameterTool.getInt("parallelism", 1);

        System.out.println("Parameters:");
        System.out.println("\tServiceUrl:\t" + serviceUrl);
        System.out.println("\tInputTopic:\t" + inputTopic);
        System.out.println("\tSubscription:\t" + subscription);
        System.out.println("\tOutputTopic:\t" + outputTopic);
        System.out.println("\tParallelism:\t" + parallelism);

        PulsarSourceBuilder<String> builder = PulsarSourceBuilder.builder(new SimpleStringSchema())
                .serviceUrl(serviceUrl)
                .topic(inputTopic)
                .subscriptionName(subscription);
        SourceFunction<String> src = builder.build();
        DataStream<String> input = env.addSource(src);


        DataStream<WordWithCount> wc = input
                .flatMap((FlatMapFunction<String, WordWithCount>) (line, collector) -> {
                    for (String word : line.split("\\s")) {
                        collector.collect(
                                WordWithCount.newBuilder().setWord(word).setCount(1).build()
                        );
                    }
                })
                .returns(WordWithCount.class)
                .keyBy(ROUTING_KEY)
                .timeWindow(Time.seconds(5))
                .reduce((ReduceFunction<WordWithCount>) (c1, c2) ->
                        WordWithCount.newBuilder().setWord(c1.getWord()).setCount(c1.getCount() + c2.getCount()).build()
                );

        tableEnvironment.registerDataStream("wc",wc);
        Table table = tableEnvironment.sqlQuery("select word, `count` from wc");
        table.printSchema();
        TableSink sink = null;
        if (null != outputTopic) {
            sink = new PulsarAvroTableSink(serviceUrl, outputTopic, new AuthenticationDisabled(), ROUTING_KEY, WordWithCount.class);
        } else {
            // print the results with a csv file
            sink = new CsvTableSink("./examples/file",  "|");
        }
        table.writeToSink(sink);

        env.execute("Pulsar Stream WordCount");
    }

}
