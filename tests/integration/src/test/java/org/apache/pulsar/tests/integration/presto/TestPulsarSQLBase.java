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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Stopwatch;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarSQLTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testcontainers.shaded.okhttp3.OkHttpClient;
import org.testcontainers.shaded.okhttp3.Request;
import org.testcontainers.shaded.okhttp3.Response;
import org.testng.Assert;
import org.testng.annotations.DataProvider;


/**
 * Pulsar SQL test base.
 */
@Slf4j
public class TestPulsarSQLBase extends PulsarSQLTestSuite {

    protected void pulsarSQLBasicTest(TopicName topic,
                                      boolean isBatch,
                                      boolean useNsOffloadPolices,
                                      Schema schema,
                                      CompressionType compressionType) throws Exception {
        log.info("Pulsar SQL basic test. topic: {}", topic);

        waitPulsarSQLReady();

        log.info("start prepare data for query. topic: {}", topic);
        int messageCnt = prepareData(topic, isBatch, useNsOffloadPolices, schema, compressionType);
        log.info("finish prepare data for query. topic: {}, messageCnt: {}", topic, messageCnt);

        validateMetadata(topic);

        validateData(topic, messageCnt, schema);

        log.info("Finish Pulsar SQL basic test. topic: {}", topic);
    }

    @DataProvider(name = "batchingAndCompression")
    public static Object[][] batchingAndCompression() {
        return new Object[][] {
                { true, CompressionType.ZLIB },
                { true, CompressionType.ZSTD },
                { true, CompressionType.SNAPPY },
                { true, CompressionType.LZ4 },
                { true, CompressionType.NONE },
                { false, CompressionType.ZLIB },
                { false, CompressionType.ZSTD },
                { false, CompressionType.SNAPPY },
                { false, CompressionType.LZ4 },
                { false, CompressionType.NONE },
        };
    }

    public void waitPulsarSQLReady() throws Exception {
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

        // check presto follow workers start finish.
        if (pulsarCluster.getSqlFollowWorkerContainers() != null
                && pulsarCluster.getSqlFollowWorkerContainers().size() > 0) {
            OkHttpClient okHttpClient = new OkHttpClient();
            Request request = new Request.Builder()
                    .url("http://" + pulsarCluster.getPrestoWorkerContainer().getUrl() + "/v1/node")
                    .build();
            do {
                try (Response response = okHttpClient.newCall(request).execute()) {
                    Assert.assertNotNull(response.body());
                    String nodeJsonStr = response.body().string();
                    Assert.assertTrue(nodeJsonStr.length() > 0);
                    log.info("presto node info: {}", nodeJsonStr);
                    if (nodeJsonStr.contains("uri")) {
                        log.info("presto node exist.");
                        break;
                    }
                    Thread.sleep(1000);
                }
            } while (true);
        }
    }

    protected int prepareData(TopicName topicName,
                              boolean isBatch,
                              boolean useNsOffloadPolices,
                              Schema schema,
                              CompressionType compressionType) throws Exception {
        throw new Exception("Unsupported operation prepareData.");
    }

    public void validateMetadata(TopicName topicName) throws Exception {
        ContainerExecResult result = execQuery("show schemas in pulsar;");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains(topicName.getNamespace());

        pulsarCluster.getBroker(0)
                .execCmd(
                        "/bin/bash",
                        "-c", "bin/pulsar-admin namespaces unload " + topicName.getNamespace());

        Awaitility.await().untilAsserted(
                () -> {
                    ContainerExecResult r = execQuery(
                            String.format("show tables in pulsar.\"%s\";", topicName.getNamespace()));
                    assertThat(r.getExitCode()).isEqualTo(0);
                    // the show tables query return lowercase table names, so ignore case
                    assertThat(r.getStdout()).containsIgnoringCase(topicName.getLocalName());
                }
        );
    }

    protected void validateContent(int messageNum, String[] contentArr, Schema schema) throws Exception {
        throw new Exception("Unsupported operation validateContent.");
    }

    private void validateData(TopicName topicName, int messageNum, Schema schema) throws Exception {
        String namespace = topicName.getNamespace();
        String topic = topicName.getLocalName();

        final String queryAllDataSql;
        if (schema.getSchemaInfo().getType().isStruct()
                || schema.getSchemaInfo().getType().equals(SchemaType.KEY_VALUE)) {
            queryAllDataSql = String.format("select * from pulsar.\"%s\".\"%s\" order by entryid;", namespace, topic);
        } else {
            queryAllDataSql = String.format("select * from pulsar.\"%s\".\"%s\";", namespace, topic);
        }

        Awaitility.await()
                // first poll immediately
                .pollDelay(Duration.ofMillis(0))
                // use relatively long poll interval so that polling doesn't consume too much resources
                .pollInterval(Duration.ofSeconds(3))
                // retry up to 15 seconds from first attempt
                .atMost(Duration.ofSeconds(15))
                .untilAsserted(
                () -> {
                    ContainerExecResult containerExecResult = execQuery(queryAllDataSql);
                    assertThat(containerExecResult.getExitCode()).isEqualTo(0);
                    log.info("select sql query output \n{}", containerExecResult.getStdout());
                    String[] split = containerExecResult.getStdout().split("\n");
                    assertThat(split.length).isEqualTo(messageNum);
                    String[] contentArr = containerExecResult.getStdout().split("\n|,");
                    validateContent(messageNum, contentArr, schema);
                }
        );

        // test predicate pushdown
        String query = String.format("select * from pulsar" +
                ".\"%s\".\"%s\" order by __publish_time__", namespace, topic);
        log.info("Executing query: {}", query);
        ResultSet res = connection.createStatement().executeQuery(query);

        List<Timestamp> timestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            timestamps.add(res.getTimestamp("__publish_time__"));
        }
        log.info("Executing query: result for topic {} timestamps size {}", topic, timestamps.size());

        assertThat(timestamps.size()).isGreaterThan(messageNum - 2);

        query = String.format("select * from pulsar" +
                ".\"%s\".\"%s\" where __publish_time__ > timestamp '%s' order by __publish_time__",
                namespace, topic, timestamps.get(timestamps.size() / 2));
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        List<Timestamp> returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        log.info("Executing query: result for topic {} returnedTimestamps size: {}", topic, returnedTimestamps.size());
        if (timestamps.size() % 2 == 0) {
            // for example: total size 10, the right receive number is 4, so 4 + 1 == 10 / 2
            assertThat(returnedTimestamps.size() + 1).isEqualTo(timestamps.size() / 2);
        } else {
            // for example: total size 101, the right receive number is 50, so 50 == (101 - 1) / 2
            assertThat(returnedTimestamps.size()).isEqualTo((timestamps.size() - 1) / 2);
        }

        // Try with a predicate that has a earlier time than any entry
        // Should return all rows
        query = String.format("select * from pulsar.\"%s\".\"%s\" where "
                + "__publish_time__ > from_unixtime(%s) order by __publish_time__", namespace, topic, 0);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        log.info("Executing query: result for topic {} returnedTimestamps size: {}", topic, returnedTimestamps.size());
        assertThat(returnedTimestamps.size()).isEqualTo(timestamps.size());

        // Try with a predicate that has a latter time than any entry
        // Should return no rows

        query = String.format("select * from pulsar.\"%s\".\"%s\" where "
                + "__publish_time__ > from_unixtime(%s) order by __publish_time__", namespace, topic, 99999999999L);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        log.info("Executing query: result for topic {} returnedTimestamps size: {}", topic, returnedTimestamps.size());
        assertThat(returnedTimestamps.size()).isEqualTo(0);

        int count = selectCount(namespace, topic);
        assertThat(count).isGreaterThan(messageNum - 2);
    }

    public ContainerExecResult execQuery(final String query) throws Exception {
        ContainerExecResult containerExecResult;

        containerExecResult = pulsarCluster.getPrestoWorkerContainer()
                .execCmd("/bin/bash", "-c", PulsarCluster.PULSAR_COMMAND_SCRIPT + " sql --execute " + "'" + query + "'");

        Stopwatch sw = Stopwatch.createStarted();
        while (containerExecResult.getExitCode() != 0 && sw.elapsed(TimeUnit.SECONDS) < 120) {
            TimeUnit.MILLISECONDS.sleep(500);
            containerExecResult = pulsarCluster.getPrestoWorkerContainer()
                    .execCmd("/bin/bash", "-c", PulsarCluster.PULSAR_COMMAND_SCRIPT + " sql --execute " + "'" + query + "'");
        }

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

    protected int selectCount(String namespace, String tableName) throws SQLException {
        String query = String.format("select count(*) from pulsar.\"%s\".\"%s\"", namespace, tableName);
        log.info("Executing count query: {}", query);
        ResultSet res = connection.createStatement().executeQuery(query);
        res.next();
        return res.getInt("_col0");
    }

}
