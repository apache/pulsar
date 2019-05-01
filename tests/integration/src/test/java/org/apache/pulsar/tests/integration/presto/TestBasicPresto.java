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
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestBasicPresto extends PulsarTestSuite {

    private static final int NUM_OF_STOCKS = 10;

    @BeforeClass
    public void setupPresto() throws Exception {
        pulsarCluster.startPrestoWorker();
    }

    @AfterClass
    public void teardownPresto() {
        log.info("tearing down...");
        pulsarCluster.stopPrestoWorker();
    }

    @Test
    public void testSimpleSQLQueryBatched() throws Exception {
        testSimpleSQLQuery(true);
    }

    @Test
    public void testSimpleSQLQueryNonBatched() throws Exception {
        testSimpleSQLQuery(false);
    }
    
    public void testSimpleSQLQuery(boolean isBatched) throws Exception {

        // wait until presto worker started
        ContainerExecResult result;
        do {
            try {
                result = execQuery("show catalogs;");
                assertThat(result.getExitCode()).isEqualTo(0);
                assertThat(result.getStdout()).contains("pulsar", "system");
                break;
            } catch (ContainerExecException cee) {
                if (cee.getResult().getStderr().contains("Presto server is still initializing")) {
                    Thread.sleep(10000);
                } else {
                    throw cee;
                }
            }
        } while (true);

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                                    .build();

        final String stocksTopic = "stocks";

        @Cleanup
        Producer<Stock> producer = pulsarClient.newProducer(JSONSchema.of(Stock.class))
                .topic(stocksTopic)
                .enableBatching(isBatched)
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            final Stock stock = new Stock(i,"STOCK_" + i , 100.0 + i * 10);
            producer.send(stock);
        }

        result = execQuery("show schemas in pulsar;");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains("public/default");

        result = execQuery("show tables in pulsar.\"public/default\";");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains("stocks");

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
