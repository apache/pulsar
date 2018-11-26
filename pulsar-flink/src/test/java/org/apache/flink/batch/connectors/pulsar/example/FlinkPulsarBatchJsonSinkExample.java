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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.batch.connectors.pulsar.PulsarJsonOutputFormat;

import java.util.Arrays;
import java.util.List;

/**
 * Implements a batch program on Pulsar topic by writing Flink DataSet as Json.
 */
public class FlinkPulsarBatchJsonSinkExample {

    private static final List<NasaMission> nasaMissions = Arrays.asList(
            new NasaMission(1, "Mercury program", 1959, 1963),
            new NasaMission(2, "Apollo program", 1961, 1972),
            new NasaMission(3, "Gemini program", 1963, 1966),
            new NasaMission(4, "Skylab", 1973, 1974),
            new NasaMission(5, "Apollo–Soyuz Test Project", 1975, 1975));

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String TOPIC_NAME = "my-flink-topic";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create PulsarJsonOutputFormat instance
        final OutputFormat<NasaMission> pulsarJsonOutputFormat = new PulsarJsonOutputFormat<>(SERVICE_URL, TOPIC_NAME);

        // create DataSet
        DataSet<NasaMission> nasaMissionDS = env.fromCollection(nasaMissions);
        // map nasa mission names to upper-case
        nasaMissionDS.map(nasaMission -> new NasaMission(
                nasaMission.id,
                nasaMission.missionName.toUpperCase(),
                nasaMission.startYear,
                nasaMission.endYear))
        // filter missions which started after 1970
        .filter(nasaMission -> nasaMission.startYear > 1970)
        // write batch data to Pulsar
        .output(pulsarJsonOutputFormat);

        // set parallelism to write Pulsar in parallel (optional)
        env.setParallelism(2);

        // execute program
        env.execute("Flink - Pulsar Batch Json");
    }

    /**
     * NasaMission data model
     *
     * Note: Properties should be public or have getter functions to be visible
     */
    private static class NasaMission {

        private int id;
        private String missionName;
        private int startYear;
        private int endYear;

        public NasaMission(int id, String missionName, int startYear, int endYear) {
            this.id = id;
            this.missionName = missionName;
            this.startYear = startYear;
            this.endYear = endYear;
        }

        public int getId() {
            return id;
        }

        public String getMissionName() {
            return missionName;
        }

        public int getStartYear() {
            return startYear;
        }

        public int getEndYear() {
            return endYear;
        }
    }

}