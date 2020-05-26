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
import org.apache.pulsar.tests.integration.containers.DebeziumSqlServerContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

import java.io.Closeable;
import java.util.Map;

/**
 * A tester for testing Debezium SqlServer source.
 *
 * It reads binlog from SqlServer, and store the debezium output into Pulsar.
 * This test verify that the target topic contains wanted number messages.
 *
 * Debezium SqlServer Container is "microsoft/mssql-server-linux:2017-CU9-GDR2",
 * which is a SqlServer database server preconfigured with an inventory database.
 */
@Slf4j
public class DebeziumSqlServerSourceTester extends SourceTester<DebeziumSqlServerContainer> implements Closeable {

    private static final String NAME = "debezium-sqlserver";

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumSqlServerContainer debeziumSqlServerContainer;

    private final PulsarCluster pulsarCluster;

    public DebeziumSqlServerSourceTester(PulsarCluster cluster, String converterClassName) {
        super(NAME);
        this.pulsarCluster = cluster;
        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

        sourceConfig.put("database.hostname", DebeziumSqlServerContainer.NAME);
        sourceConfig.put("database.port", "1433");
        sourceConfig.put("database.user", "sa");
        sourceConfig.put("database.password", "Password!");
        sourceConfig.put("database.dbname","testDB");
        sourceConfig.put("database.server.name", "dbserver1");
        sourceConfig.put("pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("key.converter", converterClassName);
        sourceConfig.put("value.converter", converterClassName);
        sourceConfig.put("topic.namespace", "debezium/sqlserver-" +
                (converterClassName.endsWith("AvroConverter") ? "avro" : "json"));
    }

    @Override
    public void setServiceContainer(DebeziumSqlServerContainer container) {
        log.info("start debezium sqlserver server container.");
        debeziumSqlServerContainer = container;
        pulsarCluster.startService(DebeziumSqlServerContainer.NAME, debeziumSqlServerContainer);
    }

    @Override
    public void prepareSource() throws Exception {
        this.debeziumSqlServerContainer.execCmd("cat debezium-sqlserver-init/inventory.sql | docker exec -i " + DebeziumSqlServerContainer.NAME + " bash -c '/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD'");
        log.info("debezium sqlserver server already contains preconfigured data.");
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        this.debeziumSqlServerContainer.execCmd(
                "/bin/bash", "-c",
                "/opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U sa -P 'Password!' -e 'SELECT * FROM inventory.products'");
        this.debeziumSqlServerContainer.execCmd(
                "/bin/bash", "-c",
                "/opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U sa -P 'Password!' " +
                        "-e \"INSERT INTO inventory.products(name, description, weight) " +
                        "values('test-debezium', 'This is description', 2.0)\"");
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        this.debeziumSqlServerContainer.execCmd(
                "/bin/bash", "-c",
                "/opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U sa -P 'Password!' " +
                        "-e \"UPDATE inventory.products set description='update description', weight=10 " +
                        "WHERE name='test-debezium'\"");
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        this.debeziumSqlServerContainer.execCmd(
                "/bin/bash", "-c",
                "/opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U sa -P 'Password!'  -e 'SELECT * FROM inventory.products'");
        this.debeziumSqlServerContainer.execCmd(
                "/bin/bash", "-c",
                "/opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U sa -P 'Password!'  " +
                        "-e \"DELETE FROM inventory.products WHERE name='test-debezium'\"");
        this.debeziumSqlServerContainer.execCmd(
                "/bin/bash", "-c",
                "/opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U sa -P 'Password!'  -e 'SELECT * FROM inventory.products'");
    }


    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        log.info("debezium sqlserver server already contains preconfigured data.");
        return null;
    }

    @Override
    public void close() {
        if (pulsarCluster != null) {
            pulsarCluster.stopService(DebeziumSqlServerContainer.NAME, debeziumSqlServerContainer);
        }
    }

}
