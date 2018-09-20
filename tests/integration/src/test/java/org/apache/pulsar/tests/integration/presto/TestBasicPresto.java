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
package org.apache.pulsar.tests.integration.presto;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestBasicPresto extends PulsarClusterTestBase {

    private static final int NUM_OF_STOCKS = 10;

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .enablePrestoWorker(true)
                .clusterName(clusterName)
                .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup with presto worker", spec.clusterName());
    }

    @Test
    public void testDefaultCatalog() throws Exception {
        ContainerExecResult containerExecResult = execQuery("show catalogs;");
        assertThat(containerExecResult.getExitCode()).isEqualTo(0);
        assertThat(containerExecResult.getStdout()).contains("pulsar", "system");
    }

    @Test
    public void testSimpleSQLQuery() throws Exception {

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                                    .build();

        final String stocksTopic = "stocks";

        @Cleanup
        Producer<Stock> producer = pulsarClient.newProducer(JSONSchema.of(Stock.class))
                .topic(stocksTopic)
                .create();


        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            final Stock stock = new Stock(i,"STOCK_" + i , 100.0 + i * 10);
            producer.send(stock);
        }

        ContainerExecResult containerExecResult = execQuery("select * from pulsar.\"public/default\".stocks order by entryid;");
        assertThat(containerExecResult.getExitCode()).isEqualTo(0);
        log.info("select sql query output \n{}", containerExecResult.getStdout());
        String[] split = containerExecResult.getStdout().split("\n");
        assertThat(split.length).isGreaterThan(NUM_OF_STOCKS - 2);

        String[] split2 = containerExecResult.getStdout().split("\n|,");

        for (int i = 0; i < NUM_OF_STOCKS - 2; ++i) {
            assertThat(split2).contains("\"" + i + "\"");
            assertThat(split2).contains("\"" + "STOCK_" + i + "\"");
            assertThat(split2).contains("\"" + (100.0 + i * 10) + "\"");
        }

    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    public static ContainerExecResult execQuery(final String query) throws Exception {
        ContainerExecResult containerExecResult;

        containerExecResult = pulsarCluster.getPrestoWorkerContainer()
                .execCmd("/bin/bash", "-c", PulsarCluster.PULSAR_COMMAND_SCRIPT + " sql --execute " + "'" + query + "'");

        return containerExecResult;

    }

}
