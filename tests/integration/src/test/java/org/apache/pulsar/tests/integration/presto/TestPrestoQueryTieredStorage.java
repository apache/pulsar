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
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.S3Container;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.shaded.org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestPrestoQueryTieredStorage extends PulsarTestSuite {

    private final static int ENTRIES_PER_LEDGER = 1024;
    private final static String OFFLOAD_DRIVER = "s3";
    private final static String BUCKET = "pulsar-integtest";
    private final static String ENDPOINT = "http://" + S3Container.NAME + ":9090";

    private S3Container s3Container;

    @Override
    protected void beforeStartCluster() throws Exception {
        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            getEnv().forEach(brokerContainer::withEnv);
        }
    }

    @BeforeClass
    public void setupPresto() throws Exception {
        s3Container = new S3Container(
                pulsarCluster.getClusterName(),
                S3Container.NAME)
                .withNetwork(pulsarCluster.getNetwork())
                .withNetworkAliases(S3Container.NAME);
        s3Container.start();

        log.info("[setupPresto] prestoWorker: " + pulsarCluster.getPrestoWorkerContainer());
        pulsarCluster.startPrestoWorker(OFFLOAD_DRIVER, getOffloadProperties(BUCKET, null, ENDPOINT));
    }

    public String getOffloadProperties(String bucket, String region, String endpoint) {
        checkNotNull(bucket);
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"s3ManagedLedgerOffloadBucket\":").append("\"").append(bucket).append("\",");
        if (StringUtils.isNotEmpty(region)) {
            sb.append("\"s3ManagedLedgerOffloadRegion\":").append("\"").append(region).append("\",");
        }
        if (StringUtils.isNotEmpty(endpoint)) {
            sb.append("\"s3ManagedLedgerOffloadServiceEndpoint\":").append("\"").append(endpoint).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }


    @AfterClass
    public void teardownPresto() {
        log.info("tearing down...");
        if (null != s3Container) {
            s3Container.stop();
        }

        pulsarCluster.stopPrestoWorker();
    }

    // Flaky Test: https://github.com/apache/pulsar/issues/7750
    // @Test
    public void testQueryTieredStorage1() throws Exception {
        testSimpleSQLQuery(false);
    }

    // Flaky Test: https://github.com/apache/pulsar/issues/7750
    // @Test
    public void testQueryTieredStorage2() throws Exception {
        testSimpleSQLQuery(true);
    }

    public void testSimpleSQLQuery(boolean isNamespaceOffload) throws Exception {

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

        String stocksTopic = "stocks-" + randomName(5);

        @Cleanup
        Producer<Stock> producer = pulsarClient.newProducer(JSONSchema.of(Stock.class))
                .topic(stocksTopic)
                .create();

        long firstLedgerId = -1;
        long currentLedgerId = -1;
        int sendMessageCnt = 0;
        while (currentLedgerId <= firstLedgerId) {
            sendMessageCnt ++;
            final Stock stock = new Stock(sendMessageCnt,"STOCK_" + sendMessageCnt , 100.0 + sendMessageCnt * 10);
            MessageIdImpl messageId = (MessageIdImpl) producer.send(stock);
            if (firstLedgerId == -1) {
                firstLedgerId = messageId.getLedgerId();
            }
            currentLedgerId = messageId.getLedgerId();
            log.info("firstLedgerId: {}, currentLedgerId: {}", firstLedgerId, currentLedgerId);
            Thread.sleep(100);
        }
        producer.flush();

        offloadAndDeleteFromBK(isNamespaceOffload, stocksTopic);

        // check schema
        result = execQuery("show schemas in pulsar;");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains("public/default");

        // check table
        result = execQuery("show tables in pulsar.\"public/default\";");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains(stocksTopic);

        // check query
        ContainerExecResult containerExecResult = execQuery(String.format("select * from pulsar.\"public/default\".%s order by entryid;", stocksTopic));
        assertThat(containerExecResult.getExitCode()).isEqualTo(0);
        log.info("select sql query output \n{}", containerExecResult.getStdout());
        String[] split = containerExecResult.getStdout().split("\n");
        assertThat(split.length).isGreaterThan(sendMessageCnt - 2);

        String[] split2 = containerExecResult.getStdout().split("\n|,");

        for (int i = 0; i < sendMessageCnt - 2; ++i) {
            assertThat(split2).contains("\"" + i + "\"");
            assertThat(split2).contains("\"" + "STOCK_" + i + "\"");
            assertThat(split2).contains("\"" + (100.0 + i * 10) + "\"");
        }

        // test predicate pushdown

        String url = String.format("jdbc:presto://%s",  pulsarCluster.getPrestoWorkerContainer().getUrl());
        Connection connection = DriverManager.getConnection(url, "test", null);

        String query = String.format("select * from pulsar" +
                ".\"public/default\".%s order by __publish_time__", stocksTopic);
        log.info("Executing query: {}", query);
        ResultSet res = connection.createStatement().executeQuery(query);

        List<Timestamp> timestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            timestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(timestamps.size()).isGreaterThan(sendMessageCnt - 2);

        query = String.format("select * from pulsar" +
                ".\"public/default\".%s where __publish_time__ > timestamp '%s' order by __publish_time__", stocksTopic, timestamps.get(timestamps.size() / 2));
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        List<Timestamp> returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(timestamps.size() / 2);

        // Try with a predicate that has a earlier time than any entry
        // Should return all rows
        query = String.format("select * from pulsar" +
                ".\"public/default\".%s where __publish_time__ > from_unixtime(%s) order by __publish_time__", stocksTopic, 0);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(timestamps.size());

        // Try with a predicate that has a latter time than any entry
        // Should return no rows

        query = String.format("select * from pulsar" +
                ".\"public/default\".%s where __publish_time__ > from_unixtime(%s) order by __publish_time__", stocksTopic, 99999999999L);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(0);
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

    private static void printCurrent(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            String columnValue = rs.getString(i);
            System.out.print(columnValue + " " + rsmd.getColumnName(i));
        }
        System.out.println("");

    }

    private void offloadAndDeleteFromBK(boolean isNamespaceOffload, String stocksTopic) {
        String adminUrl = pulsarCluster.getHttpServiceUrl();
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            // read managed ledger info, check ledgers exist
            long firstLedger = admin.topics().getInternalStats(stocksTopic).ledgers.get(0).ledgerId;

            String output = "";

            if (isNamespaceOffload) {
                pulsarCluster.runAdminCommandOnAnyBroker(
                        "namespaces", "set-offload-policies",
                        "--bucket", "pulsar-integtest",
                        "--driver", "s3",
                        "--endpoint", "http://" + S3Container.NAME + ":9090",
                        "--offloadAfterElapsed", "1000",
                        "public/default");

                output = pulsarCluster.runAdminCommandOnAnyBroker(
                        "namespaces", "get-offload-policies").getStdout();
                Assert.assertTrue(output.contains("pulsar-integtest"));
                Assert.assertTrue(output.contains("s3"));
            }

            // offload with a low threshold
            output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload", "--size-threshold", "1M", stocksTopic).getStdout();
            Assert.assertTrue(output.contains("Offload triggered"));

            output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload-status", "-w", stocksTopic).getStdout();
            Assert.assertTrue(output.contains("Offload was a success"));

            // delete the first ledger, so that we cannot possibly read from it
            ClientConfiguration bkConf = new ClientConfiguration();
            bkConf.setZkServers(pulsarCluster.getZKConnString());
            try (BookKeeper bk = new BookKeeper(bkConf)) {
                bk.deleteLedger(firstLedger);
            } catch (Exception e) {
                log.error("Failed to delete from BookKeeper.", e);
                Assert.fail("Failed to delete from BookKeeper.");
            }

            // Unload topic to clear all caches, open handles, etc
            admin.topics().unload(stocksTopic);
        } catch (Exception e) {
            Assert.fail("Failed to deleteOffloadedDataFromBK.");
        }
    }

    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("managedLedgerMaxEntriesPerLedger", String.valueOf(ENTRIES_PER_LEDGER));
        result.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        result.put("managedLedgerOffloadDriver", OFFLOAD_DRIVER);
        result.put("s3ManagedLedgerOffloadBucket", BUCKET);
        result.put("s3ManagedLedgerOffloadServiceEndpoint", ENDPOINT);

        return result;
    }

}
