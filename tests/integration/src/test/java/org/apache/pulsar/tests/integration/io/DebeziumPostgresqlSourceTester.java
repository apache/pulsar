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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.tests.integration.containers.DebeziumPostgresqlContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A tester for testing Debezium Postgresql source.
 *
 * It reads binlog from Postgres, and store the debezium output into Pulsar.
 * This test verify that the target topic contains wanted number messages.
 *
 * Debezium Postgresql Container is "debezium/example-postgres:0.10",
 * which is a Postgresql database server preconfigured with an inventory database.
 */
@Slf4j
public class DebeziumPostgresqlSourceTester extends SourceTester<DebeziumPostgresqlContainer> implements Closeable {

    private static final String NAME = "debezium-postgres";

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumPostgresqlContainer debeziumPostgresqlContainer;

    private final PulsarCluster pulsarCluster;

    public DebeziumPostgresqlSourceTester(PulsarCluster cluster) {
        super(NAME);
        this.pulsarCluster = cluster;
        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

        sourceConfig.put("database.hostname", DebeziumPostgresqlContainer.NAME);
        sourceConfig.put("database.port", "5432");
        sourceConfig.put("database.user", "postgres");
        sourceConfig.put("database.password", "postgres");
        sourceConfig.put("database.server.id", "184055");
        sourceConfig.put("database.server.name", "dbserver1");
        sourceConfig.put("database.dbname", "postgres");
        sourceConfig.put("schema.whitelist", "inventory");
        sourceConfig.put("pulsar.service.url", pulsarServiceUrl);
    }

    @Override
    public void setServiceContainer(DebeziumPostgresqlContainer container) {
        log.info("start debezium postgresql server container.");
        debeziumPostgresqlContainer = container;
        pulsarCluster.startService(DebeziumPostgresqlContainer.NAME, debeziumPostgresqlContainer);
    }

    @Override
    public void prepareSource() throws Exception {
        log.info("debezium postgresql server already contains preconfigured data.");
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        log.info("debezium postgresql server already contains preconfigured data.");
        return null;
    }

    public void validateSourceResult(Consumer<KeyValue<byte[], byte[]>> consumer, int number) throws Exception {
        int recordsNumber = 0;
        Message<KeyValue<byte[], byte[]>> msg = consumer.receive(2, TimeUnit.SECONDS);
        while(msg != null) {
            recordsNumber ++;
            log.info("Received message: {}.", msg.getValue());
            String key = new String(msg.getValue().getKey());
            String value = new String(msg.getValue().getValue());
            Assert.assertTrue(key.contains("dbserver1.inventory.products.Key"));
            Assert.assertTrue(value.contains("dbserver1.inventory.products.Value"));
            consumer.acknowledge(msg);
            msg = consumer.receive(1, TimeUnit.SECONDS);
        }

        Assert.assertEquals(recordsNumber, number);
        log.info("Stop debezium postgresql server container. topic: {} has {} records.", consumer.getTopic(), recordsNumber);
    }

    @Override
    public void close() {
        if (pulsarCluster != null) {
            pulsarCluster.stopService(DebeziumPostgresqlContainer.NAME, debeziumPostgresqlContainer);
        }
    }

}
