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
package org.apache.pulsar.io.debezium;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Map;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the implementation of {@link PulsarDatabaseHistory}.
 */
public class PulsarDatabaseHistoryTest extends ProducerConsumerBase {

    private PulsarDatabaseHistory history;
    private Map<String, Object> position;
    private Map<String, String> source;
    private String topicName;
    private String ddl;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        source = Collect.hashMapOf("server", "my-server");
        setLogPosition(0);
        this.topicName = "persistent://my-property/my-ns/schema-changes-topic";
        this.history = new PulsarDatabaseHistory();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private void testHistoryTopicContent(boolean skipUnparseableDDL, boolean testWithClientBuilder) throws Exception {
        Configuration.Builder configBuidler = Configuration.create()
                .with(PulsarDatabaseHistory.TOPIC, topicName)
                .with(DatabaseHistory.NAME, "my-db-history")
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, skipUnparseableDDL);

        if (testWithClientBuilder) {
            ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerUrl.toString());
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(bao)) {
                oos.writeObject(builder);
                oos.flush();
                byte[] data = bao.toByteArray();
                configBuidler.with(PulsarDatabaseHistory.CLIENT_BUILDER, Base64.getEncoder().encodeToString(data));
            }
        } else {
            configBuidler.with(PulsarDatabaseHistory.SERVICE_URL, brokerUrl.toString());
        }

        // Start up the history ...
        history.configure(configBuidler.build(), null, DatabaseHistoryListener.NOOP, true);
        history.start();

        // Should be able to call start more than once ...
        history.start();

        history.initializeStorage();

        // Calling it another time to ensure we can work with the DB history topic already existing
        history.initializeStorage();

        DdlParser recoveryParser = new MySqlAntlrDdlParser();
        DdlParser ddlParser = new MySqlAntlrDdlParser();
        ddlParser.setCurrentSchema("db1"); // recover does this, so we need to as well
        Tables tables1 = new Tables();
        Tables tables2 = new Tables();
        Tables tables3 = new Tables();

        // Recover from the very beginning ...
        setLogPosition(0);
        history.recover(source, position, tables1, recoveryParser);

        // There should have been nothing to recover ...
        assertEquals(tables1.size(), 0);

        // Now record schema changes, which writes out to kafka but doesn't actually change the Tables ...
        setLogPosition(10);
        ddl = "CREATE TABLE foo ( first VARCHAR(22) NOT NULL ); \n" +
            "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
            "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, description VARCHAR(255) NOT NULL ); \n";
        history.record(source, position, "db1", ddl);

        // Parse the DDL statement 3x and each time update a different Tables object ...
        ddlParser.parse(ddl, tables1);
        assertEquals(3, tables1.size());
        ddlParser.parse(ddl, tables2);
        assertEquals(3, tables2.size());
        ddlParser.parse(ddl, tables3);
        assertEquals(3, tables3.size());

        // Record a drop statement and parse it for 2 of our 3 Tables...
        setLogPosition(39);
        ddl = "DROP TABLE foo;";
        history.record(source, position, "db1", ddl);
        ddlParser.parse(ddl, tables2);
        assertEquals(2, tables2.size());
        ddlParser.parse(ddl, tables3);
        assertEquals(2, tables3.size());

        // Record another DDL statement and parse it for 1 of our 3 Tables...
        setLogPosition(10003);
        ddl = "CREATE TABLE suppliers ( supplierId INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);";
        history.record(source, position, "db1", ddl);
        ddlParser.parse(ddl, tables3);
        assertEquals(3, tables3.size());

        // Stop the history (which should stop the producer) ...
        history.stop();
        history = new PulsarDatabaseHistory();
        history.configure(configBuidler.build(), null, DatabaseHistoryListener.NOOP, true);
        // no need to start

        // Recover from the very beginning to just past the first change ...
        Tables recoveredTables = new Tables();
        setLogPosition(15);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables1);

        // Recover from the very beginning to just past the second change ...
        recoveredTables = new Tables();
        setLogPosition(50);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables2);

        // Recover from the very beginning to just past the third change ...
        recoveredTables = new Tables();
        setLogPosition(10010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables3);

        // Recover from the very beginning to way past the third change ...
        recoveredTables = new Tables();
        setLogPosition(100000010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables3);
    }

    protected void setLogPosition(int index) {
        this.position = Collect.hashMapOf("filename", "my-txn-file.log",
            "position", index);
    }

    @Test
    public void shouldStartWithEmptyTopicAndStoreDataAndRecoverAllState() throws Exception {
        // Create the empty topic ...
        testHistoryTopicContent(false, true);
    }

    @Test
    public void shouldIgnoreUnparseableMessages() throws Exception {
        try (final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
            .topic(topicName)
            .create()
        ) {
            producer.send("");
            producer.send("{\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"}");
            producer.send("{\"source\":{\"server\":\"my-server\"},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"}");
            producer.send("{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"");
            producer.send("\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"}");
            producer.send("{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"xxxDROP TABLE foo;\"}");
        }

        testHistoryTopicContent(true, true);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void shouldStopOnUnparseableSQL() throws Exception {
        try (final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
            producer.send("{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"xxxDROP TABLE foo;\"}");
        }

        testHistoryTopicContent(false, false);
    }


    @Test
    public void testExists() throws Exception {
        // happy path
        testHistoryTopicContent(true, false);
        assertTrue(history.exists());

        // Set history to use dummy topic
        Configuration config = Configuration.create()
            .with(PulsarDatabaseHistory.SERVICE_URL, brokerUrl.toString())
            .with(PulsarDatabaseHistory.TOPIC, "persistent://my-property/my-ns/dummytopic")
            .with(DatabaseHistory.NAME, "my-db-history")
            .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
            .build();

        history.configure(config, null, DatabaseHistoryListener.NOOP, true);
        history.start();

        // dummytopic should not exist yet
        assertFalse(history.exists());
    }
}
