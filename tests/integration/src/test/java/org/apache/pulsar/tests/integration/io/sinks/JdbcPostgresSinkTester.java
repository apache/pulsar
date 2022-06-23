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
package org.apache.pulsar.tests.integration.io.sinks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * A tester for testing jdbc sink.
 * This will use Postgres as DB server
 */
@Slf4j
public class JdbcPostgresSinkTester extends SinkTester<PostgreSQLContainer> {

    @Data
    public static class Foo {
        private String field1;
        private String field2;
        private int field3;
    }

    @Data
    @AllArgsConstructor
    public static class KVSchemaKey {
        private int field3;
    }

    @Data
    @AllArgsConstructor
    public static class KVSchemaValue {
        private String field1;
        private String field2;
    }

    private static final String NAME = "jdbc-postgres";
    private static final String POSTGRES = "postgres";

    private final AvroSchema<Foo> schema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
    private final String tableName = "test";
    private Connection connection;
    private boolean keyValueSchema;

    public JdbcPostgresSinkTester(boolean keyValueSchema) {
        super(NAME, SinkType.JDBC_POSTGRES);
        this.keyValueSchema = keyValueSchema;

        // container default value is test
        sinkConfig.put("userName", "test");
        sinkConfig.put("password", "test");
        sinkConfig.put("nonKey", "field2,field3");
        sinkConfig.put("key", "field1");
        sinkConfig.put("tableName", tableName);
        sinkConfig.put("batchSize", 1);
    }

    @Override
    public Schema<?> getInputTopicSchema() {
        if (keyValueSchema) {
            return Schema.AUTO_CONSUME();
        } else {
            return schema;
        }
    }

    @Override
    protected PostgreSQLContainer createSinkService(PulsarCluster cluster) {
        return (PostgreSQLContainer) new PostgreSQLContainer("postgres:14.3")
            .withNetworkAliases(POSTGRES);
    }

    @Override
    public void prepareSink() throws Exception {
        String jdbcUrl = serviceContainer.getJdbcUrl();
        // we need set postgres server address in cluster network.
        sinkConfig.put("jdbcUrl", "jdbc:postgresql://" + POSTGRES + ":5432/test");
        String driver = serviceContainer.getDriverClassName();
        Class.forName(driver);

        connection = DriverManager.getConnection(jdbcUrl, "test", "test");
        log.info("getConnection: {}, jdbcurl: {}", connection, jdbcUrl);

        // create table
        String createTable = "CREATE TABLE " + tableName +
            " (field1 TEXT, field2 TEXT, field3 INTEGER, PRIMARY KEY (field3))";
        int ret = connection.createStatement().executeUpdate(createTable);
        log.info("created table in jdbc: {}, return value: {}", createTable, ret);
    }

    @Override
    public void produceMessage(int numMessages, PulsarClient client, String inputTopicName, LinkedHashMap<String, String> kvs) throws Exception {
        if (!keyValueSchema) {
            super.produceMessage(numMessages, client, inputTopicName, kvs);
            return;
        }

        @Cleanup
        Producer<KeyValue<KVSchemaKey, KVSchemaValue>> producer = client.newProducer(Schema.KeyValue(Schema.JSON(KVSchemaKey.class),
                        Schema.AVRO(KVSchemaValue.class), KeyValueEncodingType.SEPARATED))
                .topic(inputTopicName)
                .create();

        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            kvs.put(key, key);
            producer.newMessage()
                    .value(new KeyValue<>(new KVSchemaKey(i), new KVSchemaValue("f1_" + i, "f2_" + i)))
                    .send();
        }

    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        log.info("Query table content from postgres server: {}", tableName);
        String querySql = "SELECT * FROM " + tableName + " ORDER BY field3";
        ResultSet rs;
        try {
            // backend flush may not complete.
            Thread.sleep(1000);

            PreparedStatement statement = connection.prepareStatement(querySql,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
            rs = statement.executeQuery();

            if (!keyValueSchema && kvs.get("ACTION").equals("DELETE")) {
                assertFalse(rs.first());
                return;
            }
            int index = 0;
            while (rs.next()) {
                String field1 = rs.getString(1);
                String field2 = rs.getString(2);
                int field3 = rs.getInt(3);
                if (keyValueSchema) {
                    assertEquals(field1, "f1_" + index);
                    assertEquals(field2, "f2_" + index);
                    assertEquals(field3, index);
                } else {
                    String value = kvs.get("key-" + field3);
                    Foo obj = schema.decode(value.getBytes());
                    assertEquals(obj.field1, field1);
                    assertEquals(obj.field2, field2);
                    assertEquals(obj.field3, field3);
                }
                index++;
            }
        } catch (Exception e) {
            log.error("Got exception: ", e);
            fail("Got exception when op sql: " + e);
        }
    }

    public boolean isKeyValueSchema() {
        return keyValueSchema;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
