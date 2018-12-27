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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.batch.connectors.pulsar.PulsarCsvOutputFormat;

import java.util.Arrays;
import java.util.List;

/**
 * Implements a batch program on Pulsar topic by writing Flink DataSet as Csv.
 */
public class FlinkPulsarBatchCsvSinkExample {

    private static final List<Tuple4<Integer, String, Integer, Integer>> nasaMissions = Arrays.asList(
            new Tuple4(1, "Mercury program", 1959, 1963),
            new Tuple4(2, "Apollo program", 1961, 1972),
            new Tuple4(3, "Gemini program", 1963, 1966),
            new Tuple4(4, "Skylab", 1973, 1974),
            new Tuple4(5, "Apollo–Soyuz Test Project", 1975, 1975));

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String TOPIC_NAME = "my-flink-topic";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create PulsarCsvOutputFormat instance
        final OutputFormat<Tuple4<Integer, String, Integer, Integer>> pulsarCsvOutputFormat =
                new PulsarCsvOutputFormat<>(SERVICE_URL, TOPIC_NAME);

        // create DataSet
        DataSet<Tuple4<Integer, String, Integer, Integer>> nasaMissionDS = env.fromCollection(nasaMissions);
        // map nasa mission names to upper-case
        nasaMissionDS.map(
            new MapFunction<Tuple4<Integer, String, Integer, Integer>, Tuple4<Integer, String, Integer, Integer>>() {
                           @Override
                           public Tuple4<Integer, String, Integer, Integer> map(
                                   Tuple4<Integer, String, Integer, Integer> nasaMission) throws Exception {
                               return new Tuple4(
                                       nasaMission.f0,
                                       nasaMission.f1.toUpperCase(),
                                       nasaMission.f2,
                                       nasaMission.f3);
                           }
                       }
        )
        // filter missions which started after 1970
        .filter(nasaMission -> nasaMission.f2 > 1970)
        // write batch data to Pulsar
        .output(pulsarCsvOutputFormat);

        // set parallelism to write Pulsar in parallel (optional)
        env.setParallelism(2);

        // execute program
        env.execute("Flink - Pulsar Batch Csv");

    }

}