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
package org.apache.pulsar.tests.integration.io.sources.debezium;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.DebeziumPostgreSqlContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.io.sources.SourceTester;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

import java.io.Closeable;
import java.util.Map;

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
public class DebeziumPostgreSqlSourceTester extends SourceTester<DebeziumPostgreSqlContainer> implements Closeable {

    private static final String NAME = "debezium-postgres";

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumPostgreSqlContainer debeziumPostgresqlContainer;

    private final PulsarCluster pulsarCluster;

    public DebeziumPostgreSqlSourceTester(PulsarCluster cluster) {
        super(NAME);
        this.pulsarCluster = cluster;
        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

        sourceConfig.put("database.hostname", DebeziumPostgreSqlContainer.NAME);
        sourceConfig.put("database.port", "5432");
        sourceConfig.put("database.user", "postgres");
        sourceConfig.put("database.password", "postgres");
        sourceConfig.put("database.server.id", "184055");
        sourceConfig.put("database.server.name", "dbserver1");
        sourceConfig.put("database.dbname", "postgres");
        sourceConfig.put("schema.whitelist", "inventory");
        sourceConfig.put("table.blacklist", "inventory.spatial_ref_sys,inventory.geom");
        sourceConfig.put("pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("topic.namespace", "debezium/postgresql");
    }

    @Override
    public void setServiceContainer(DebeziumPostgreSqlContainer container) {
        log.info("start debezium postgresql server container.");
        debeziumPostgresqlContainer = container;
        pulsarCluster.startService(DebeziumPostgreSqlContainer.NAME, debeziumPostgresqlContainer);
    }

    @Override
    public void prepareSource() {
        log.info("debezium postgresql server already contains preconfigured data.");
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        this.debeziumPostgresqlContainer.execCmd("/bin/bash", "-c",
                "psql -h 127.0.0.1 -U postgres -d postgres -c \"select * from inventory.products;\"");
        this.debeziumPostgresqlContainer.execCmd("/bin/bash", "-c",
                "psql -h 127.0.0.1 -U postgres -d postgres " +
                        "-c \"insert into inventory.products(name, description, weight) " +
                        "values('test-debezium', 'description', 10);\"");
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        this.debeziumPostgresqlContainer.execCmd("/bin/bash", "-c",
                "psql -h 127.0.0.1 -U postgres -d postgres -c \"select * from inventory.products;\"");
        this.debeziumPostgresqlContainer.execCmd("/bin/bash", "-c",
                "psql -h 127.0.0.1 -U postgres -d postgres " +
                        "-c \"delete from inventory.products where name='test-debezium';\"");
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        this.debeziumPostgresqlContainer.execCmd("/bin/bash", "-c",
                "psql -h 127.0.0.1 -U postgres -d postgres -c \"select * from inventory.products;\"");
        this.debeziumPostgresqlContainer.execCmd("/bin/bash", "-c",
                "psql -h 127.0.0.1 -U postgres -d postgres " +
                        "-c \"update inventory.products " +
                        "set description='test-update-description', weight='20' where name='test-debezium';\"");
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) {
        log.info("debezium postgresql server already contains preconfigured data.");
        return null;
    }

    @Override
    public void close() {
        if (pulsarCluster != null) {
            pulsarCluster.stopService(DebeziumPostgreSqlContainer.NAME, debeziumPostgresqlContainer);
        }
    }

}
