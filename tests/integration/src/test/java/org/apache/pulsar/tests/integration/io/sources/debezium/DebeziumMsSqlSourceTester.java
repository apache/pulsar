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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.DebeziumMsSqlContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.io.sources.SourceTester;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.junit.Assert;
import org.testcontainers.shaded.com.google.common.base.Preconditions;
import org.testng.util.Strings;

import java.io.Closeable;
import java.util.Map;

/**
 * A tester for testing Debezium Microsoft SQl Server source.
 */
@Slf4j
public class DebeziumMsSqlSourceTester extends SourceTester<DebeziumMsSqlContainer> implements Closeable {

    private static final String NAME = "debezium-mssql";

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumMsSqlContainer debeziumMsSqlContainer;

    private final PulsarCluster pulsarCluster;

    public DebeziumMsSqlSourceTester(PulsarCluster cluster) {
        super(NAME);
        this.pulsarCluster = cluster;
        this.numEntriesToInsert = 1;
        this.numEntriesExpectAfterStart = 0;

        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

        sourceConfig.put("database.hostname", DebeziumMsSqlContainer.NAME);
        sourceConfig.put("database.port", "1433");
        sourceConfig.put("database.user", "sa");
        sourceConfig.put("database.password", DebeziumMsSqlContainer.SA_PASSWORD);
        sourceConfig.put("database.server.name", "mssql");
        sourceConfig.put("database.dbname", "TestDB");
        sourceConfig.put("snapshot.mode", "schema_only");
        sourceConfig.put("database.history.pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("topic.namespace", "debezium/mssql");
    }

    @Override
    public void setServiceContainer(DebeziumMsSqlContainer container) {
        log.info("start debezium MS SQL server container.");
        Preconditions.checkState(debeziumMsSqlContainer == null);
        debeziumMsSqlContainer = container;
        pulsarCluster.startService(DebeziumMsSqlContainer.NAME, debeziumMsSqlContainer);
    }

    @SneakyThrows
    @Override
    public void prepareSource() {
        runSqlCmd("CREATE DATABASE TestDB;", false);
        runSqlCmd("EXEC sys.sp_cdc_enable_db;");
        ContainerExecResult res = runSqlCmd("SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID();");
        // " 1" to differentiate from "(1 rows affected)"
        Assert.assertTrue(res.getStdout().contains(" 1"));
        runSqlCmd("CREATE TABLE customers (" +
                "id INT NOT NULL  IDENTITY  PRIMARY KEY, " +
                "first_name VARCHAR(255) NOT NULL, " +
                "last_name VARCHAR(255) NOT NULL, " +
                "email VARCHAR(255) NOT NULL" +
                ");");
        runSqlCmd("EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customers',"
                + " @role_name = NULL, @supports_net_changes = 0, @capture_instance = 'dbo_customers_v2';");
        runSqlCmd("EXEC sys.sp_cdc_start_job;");
    }

    private ContainerExecResult runSqlCmd(String cmd) throws Exception {
        return runSqlCmd(cmd, true);
    }

    private ContainerExecResult runSqlCmd(String cmd, boolean useTestDb) throws Exception {
        log.info("Executing \"{}\"", cmd);
        ContainerExecResult response = this.debeziumMsSqlContainer
                .execCmd("/bin/bash", "-c",
                "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P \""
                        + DebeziumMsSqlContainer.SA_PASSWORD + "\" -Q \""
                        + (useTestDb ? "USE TestDB; " : "")
                        + cmd
                        + "\""
                );
        if (Strings.isNullOrEmpty(response.getStderr())) {
            log.info("Result of \"{}\":\n{}", cmd, response.getStdout());
        } else {
            log.warn("Result of \"{}\":\n{}\n{}", cmd, response.getStdout(), response.getStderr());
        }
        return response;
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        runSqlCmd("INSERT INTO customers (first_name, last_name, email) VALUES ('John', 'Doe', 'jdoe@null.dev');");
        runSqlCmd("SELECT * FROM customers WHERE last_name='Doe';");
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        runSqlCmd("DELETE FROM customers WHERE last_name='Doe';");
        runSqlCmd("SELECT * FROM customers WHERE last_name='Doe';");
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        runSqlCmd("UPDATE customers SET first_name='Jack' WHERE last_name='Doe';");
        runSqlCmd("SELECT * FROM customers WHERE last_name='Doe';");
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) {
        log.info("debezium MS SQL server already contains preconfigured data.");
        return null;
    }

    @Override
    public int initialDelayForMsgReceive() {
        return 30;
    }

    @Override
    public String keyContains() {
        return "mssql.dbo.customers.Key";
    }

    @Override
    public String valueContains() {
        return "mssql.dbo.customers.Value";
    }

    @Override
    public void close() {
        if (pulsarCluster != null) {
            PulsarCluster.stopService(DebeziumMsSqlContainer.NAME, debeziumMsSqlContainer);
        }
    }

}
