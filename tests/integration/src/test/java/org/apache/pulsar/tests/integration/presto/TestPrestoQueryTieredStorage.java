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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.containers.S3Container;
import org.testcontainers.shaded.org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test presto query from tiered storage, the Pulsar SQL is cluster mode.
 */
@Slf4j
public class TestPrestoQueryTieredStorage extends TestPulsarSQLBase {

    private final String TENANT = "presto";
    private final String NAMESPACE = "ts";

    private S3Container s3Container;

    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
        setupExtraContainers();
    }

    @Override
    public void tearDownCluster() throws Exception {
        teardownPresto();
        super.tearDownCluster();
    }

    private void setupExtraContainers() throws Exception {
        log.info("[TestPrestoQueryTieredStorage] setupExtraContainers...");
        pulsarCluster.runAdminCommandOnAnyBroker( "tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(),
                "--admin-roles", "offload-admin", TENANT);

        pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces",
                "create", "--clusters", pulsarCluster.getClusterName(),
                NamespaceName.get(TENANT, NAMESPACE).toString());

        s3Container = new S3Container(
                pulsarCluster.getClusterName(),
                S3Container.NAME)
                .withNetwork(pulsarCluster.getNetwork())
                .withNetworkAliases(S3Container.NAME);
        s3Container.start();

        String offloadProperties = getOffloadProperties(BUCKET, null, ENDPOINT);
        pulsarCluster.startPrestoWorker(OFFLOAD_DRIVER, offloadProperties);
        pulsarCluster.startPrestoFollowWorkers(1, OFFLOAD_DRIVER, offloadProperties);
        initJdbcConnection();
    }

    private String getOffloadProperties(String bucket, String region, String endpoint) {
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

    public void teardownPresto() {
        log.info("[TestPrestoQueryTieredStorage] tearing down...");
        if (null != s3Container) {
            s3Container.stop();
        }

        pulsarCluster.stopPrestoWorker();
    }

    @Test
    public void testQueryTieredStorage1() throws Exception {
        TopicName topicName = TopicName.get(
                TopicDomain.persistent.value(), TENANT, NAMESPACE, "stocks_ts_nons_" + randomName(5));
        pulsarSQLBasicTest(topicName, false, false, JSONSchema.of(Stock.class), CompressionType.NONE);
    }

    @Test
    public void testQueryTieredStorage2() throws Exception {
        TopicName topicName = TopicName.get(
                TopicDomain.persistent.value(), TENANT, NAMESPACE, "stocks_ts_ns_" + randomName(5));
        pulsarSQLBasicTest(topicName, false, true, JSONSchema.of(Stock.class), CompressionType.NONE);
    }

    @Override
    protected int prepareData(TopicName topicName,
                              boolean isBatch,
                              boolean useNsOffloadPolices,
                              Schema schema,
                              CompressionType compressionType) throws Exception {
        @Cleanup
        Consumer<Stock> consumer = pulsarClient.newConsumer(JSONSchema.of(Stock.class))
                .topic(topicName.toString())
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        @Cleanup
        Producer<Stock> producer = pulsarClient.newProducer(JSONSchema.of(Stock.class))
                .topic(topicName.toString())
                .compressionType(compressionType)
                .create();

        long firstLedgerId = -1;
        int sendMessageCnt = 0;
        while (true) {
            Stock stock = new Stock(
                    sendMessageCnt,"STOCK_" + sendMessageCnt , 100.0 + sendMessageCnt * 10);
            MessageIdImpl messageId = (MessageIdImpl) producer.send(stock);
            sendMessageCnt ++;
            if (firstLedgerId == -1) {
                firstLedgerId = messageId.getLedgerId();
            }
            if (messageId.getLedgerId() > firstLedgerId) {
                log.info("ledger rollover firstLedgerId: {}, currentLedgerId: {}",
                        firstLedgerId, messageId.getLedgerId());
                break;
            }
            Thread.sleep(100);
        }

        offloadAndDeleteFromBK(useNsOffloadPolices, topicName);
        return sendMessageCnt;
    }

    private void offloadAndDeleteFromBK(boolean useNsOffloadPolices, TopicName topicName) {
        String adminUrl = pulsarCluster.getHttpServiceUrl();
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            // read managed ledger info, check ledgers exist
            long firstLedger = admin.topics().getInternalStats(topicName.toString()).ledgers.get(0).ledgerId;

            String output = "";

            if (useNsOffloadPolices) {
                pulsarCluster.runAdminCommandOnAnyBroker(
                        "namespaces", "set-offload-policies",
                        "--bucket", "pulsar-integtest",
                        "--driver", OFFLOAD_DRIVER,
                        "--endpoint", "http://" + S3Container.NAME + ":9090",
                        "--offloadAfterElapsed", "1000",
                        topicName.getNamespace());

                output = pulsarCluster.runAdminCommandOnAnyBroker(
                        "namespaces", "get-offload-policies", topicName.getNamespace()).getStdout();
                Assert.assertTrue(output.contains("pulsar-integtest"));
                Assert.assertTrue(output.contains(OFFLOAD_DRIVER));
            }

            // offload with a low threshold
            output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload", "--size-threshold", "0", topicName.toString()).getStdout();
            Assert.assertTrue(output.contains("Offload triggered"));

            output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload-status", "-w", topicName.toString()).getStdout();
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
            admin.topics().unload(topicName.toString());
        } catch (Exception e) {
            Assert.fail("Failed to deleteOffloadedDataFromBK.");
        }
    }

    @Override
    protected void validateContent(int messageNum, String[] contentArr, Schema schema) {
        for (int i = 0; i < messageNum; ++i) {
            assertThat(contentArr).contains("\"" + i + "\"");
            assertThat(contentArr).contains("\"" + "STOCK_" + i + "\"");
            assertThat(contentArr).contains("\"" + (100.0 + i * 10) + "\"");
        }
    }

}
