/*
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

import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.DebeziumDB2DbContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.io.sources.SourceTester;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

/**
 * A tester for testing Debezium DB2 source.
 */
@Slf4j
public class DebeziumDB2DbSourceTester extends SourceTester<DebeziumDB2DbContainer> {

    private static final String NAME = "debezium-db2";
    private static final long SLEEP_AFTER_COMMAND_MS = 30_000;

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumDB2DbContainer debeziumDB2DbContainer;

    private final PulsarCluster pulsarCluster;

    public DebeziumDB2DbSourceTester(PulsarCluster cluster) {
        super(NAME);
        this.pulsarCluster = cluster;
        this.numEntriesToInsert = 1;
        this.numEntriesExpectAfterStart = 0;

        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;
        sourceConfig.put("connector.class", "io.debezium.connector.db2.Db2Connector");
        sourceConfig.put("database.hostname", DebeziumDB2DbContainer.NAME);
        sourceConfig.put("database.port", "50000");
        sourceConfig.put("database.user", "db2inst1");
        sourceConfig.put("database.password", "admin");
        sourceConfig.put("database.dbname", "mydb2");
        sourceConfig.put("topic.prefix", "stores");
        sourceConfig.put("table.include.list", "DB2INST1.STORES");

        sourceConfig.put("database.history.pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("topic.namespace", "debezium/db2");
    }

    @Override
    public void setServiceContainer(DebeziumDB2DbContainer container) {
        log.info("start debezium db2 server container.");
        Preconditions.checkState(debeziumDB2DbContainer == null);
        debeziumDB2DbContainer = container;
        pulsarCluster.startService(DebeziumDB2DbContainer.NAME, debeziumDB2DbContainer);
    }

    @SneakyThrows
    @Override
    public void prepareSource() {
        log.info("Starting DB2. Running cdcsetup.sh next");
        var lsResult = debeziumDB2DbContainer.execInContainer("/bin/bash", "-c", "/var/custom/cdcsetup.sh");
        String stdout = lsResult.getStdout();
        log.info(String.format("Output of running cdcsetup.sh is: %s", stdout));
        int exitCode = lsResult.getExitCode();
        log.info(String.format("Running : %s", debeziumDB2DbContainer.enableCdcStatement()));
        debeziumDB2DbContainer.getPreparedStatement(debeziumDB2DbContainer.enableCdcStatement())
                .execute();

        log.info(String.format("Running : %s", debeziumDB2DbContainer.createTableStatement()));
        debeziumDB2DbContainer.getPreparedStatement(debeziumDB2DbContainer.createTableStatement())
                .execute();

        log.info(String.format("Running : %s", debeziumDB2DbContainer.addCdcTableStatement()));
        debeziumDB2DbContainer.getPreparedStatement(debeziumDB2DbContainer.addCdcTableStatement())
                .execute();

        log.info(String.format("Running : %s", debeziumDB2DbContainer.insertStatement()));
        debeziumDB2DbContainer.getPreparedStatement(debeziumDB2DbContainer.insertStatement())
                .execute();

        log.info(String.format("Running : %s", debeziumDB2DbContainer.selectStatement()));
        var result = debeziumDB2DbContainer.getStoreNameResult();
        log.info("debeziumDB2DbContainer.getStoreNameResult() returned: %s", result);
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        debeziumDB2DbContainer.getPreparedStatement("INSERT INTO DB2INST1.STORES(store_name, state_id, zip_code) VALUES ('mystore2', 2, '22222')");
        debeziumDB2DbContainer.getPreparedStatement("SELECT * FROM DB2INST1.STORES WHERE store_name='mystore2'");
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        debeziumDB2DbContainer.getPreparedStatement("DELETE FROM DB2INST1.STORES WHERE store_name='mystore2'");
        debeziumDB2DbContainer.getPreparedStatement("SELECT * FROM DB2INST1.STORES WHERE store_name='mystore2'");
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        debeziumDB2DbContainer.getPreparedStatement("UPDATE DB2INST1.STORES SET zip_code='33333' WHERE store_name='mystore2'");
        debeziumDB2DbContainer.getPreparedStatement("SELECT * FROM DB2INST1.STORES WHERE store_name='mystore2'");
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) {
        log.info("debezium db2 server already contains preconfigured data.");
        return null;
    }

    @Override
    public int initialDelayForMsgReceive() {
        return 30;
    }

    @Override
    public String keyContains() {
        return "mydb2.DB2INST1.STORES.Key";
    }

    @Override
    public String valueContains() {
        return "mydb2.DB2INST1.STORES.Value";
    }

    @Override
    public void close() {
        if (pulsarCluster != null) {
            if (debeziumDB2DbContainer != null) {
                PulsarCluster.stopService(DebeziumDB2DbContainer.NAME, debeziumDB2DbContainer);
                debeziumDB2DbContainer = null;
            }
        }
    }

}
