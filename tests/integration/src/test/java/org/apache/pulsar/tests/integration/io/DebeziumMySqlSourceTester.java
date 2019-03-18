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
package org.apache.pulsar.tests.integration.io;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.tests.integration.containers.DebeziumMySQLContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;

/**
 * A tester for testing Debezium MySQL source.
 *
 * It reads binlog from MySQL, and store the debezium output into Pulsar.
 * This test verify that the target topic contains wanted number messages.
 *
 * Debezium MySQL Container is "debezium/example-mysql:0.8",
 * which is a MySQL database server preconfigured with an inventory database.
 */
@Slf4j
public class DebeziumMySqlSourceTester extends SourceTester<DebeziumMySQLContainer> implements Closeable {

    private static final String NAME = "debezium-mysql";

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumMySQLContainer debeziumMySqlContainer;

    private final PulsarCluster pulsarCluster;

    public DebeziumMySqlSourceTester(PulsarCluster cluster) {
        super(NAME);
        this.pulsarCluster = cluster;
        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

        sourceConfig.put("database.hostname", DebeziumMySQLContainer.NAME);
        sourceConfig.put("database.port", "3306");
        sourceConfig.put("database.user", "debezium");
        sourceConfig.put("database.password", "dbz");
        sourceConfig.put("database.server.id", "184054");
        sourceConfig.put("database.server.name", "dbserver1");
        sourceConfig.put("database.whitelist", "inventory");
        sourceConfig.put("pulsar.service.url", pulsarServiceUrl);
    }

    @Override
    public void setServiceContainer(DebeziumMySQLContainer container) {
        log.info("start debezium mysql server container.");
        debeziumMySqlContainer = container;
        pulsarCluster.startService(DebeziumMySQLContainer.NAME, debeziumMySqlContainer);
    }

    @Override
    public void prepareSource() throws Exception {
        log.info("debezium mysql server already contains preconfigured data.");
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        log.info("debezium mysql server already contains preconfigured data.");
        return null;
    }

    public void validateSourceResult(Consumer<String> consumer, int number) throws Exception {
        int recordsNumber = 0;
        Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
        while(msg != null) {
            recordsNumber ++;
            log.info("Received message: {}.", msg.getValue());
            Assert.assertTrue(msg.getValue().contains("dbserver1.inventory.products"));
            consumer.acknowledge(msg);
            msg = consumer.receive(1, TimeUnit.SECONDS);
        }

        Assert.assertEquals(recordsNumber, number);
        log.info("Stop debezium mysql server container. topic: {} has {} records.", consumer.getTopic(), recordsNumber);
    }

    @Override
    public void close() {
        if (pulsarCluster != null) {
            pulsarCluster.stopService(DebeziumMySQLContainer.NAME, debeziumMySqlContainer);
        }
    }

}
