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

    private static final List<Tuple4<Integer, String, String, String>> employeeTuples = Arrays.asList(
            new Tuple4(1, "John", "Tyson", "Engineering"),
            new Tuple4(2, "Pamela", "Moon", "HR"),
            new Tuple4(3, "Jim", "Sun", "Finance"),
            new Tuple4(4, "Michael", "Star", "Engineering"));

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String TOPIC_NAME = "my-flink-topic";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create PulsarCsvOutputFormat instance
        final OutputFormat<Tuple4<Integer, String, String, String>> pulsarCsvOutputFormat =
                new PulsarCsvOutputFormat<>(SERVICE_URL, TOPIC_NAME);

        // create DataSet
        DataSet<Tuple4<Integer, String, String, String>> employeeDS = env.fromCollection(employeeTuples);
        // map employees' name, surname and department as upper-case
        employeeDS.map(
                new MapFunction<Tuple4<Integer, String, String, String>, Tuple4<Integer, String, String, String>>() {
            @Override
            public Tuple4<Integer, String, String, String> map(
                    Tuple4<Integer, String, String, String> employeeTuple) throws Exception {
                return new Tuple4(employeeTuple.f0,
                        employeeTuple.f1.toUpperCase(),
                        employeeTuple.f2.toUpperCase(),
                        employeeTuple.f3.toUpperCase());
            }
        })
        // filter employees which is member of Engineering
        .filter(tuple -> tuple.f3.equals("ENGINEERING"))
        // write batch data to Pulsar
        .output(pulsarCsvOutputFormat);

        // execute program
        env.execute("Flink - Pulsar Batch Csv");

    }

}