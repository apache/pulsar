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
package org.apache.pulsar.broker.admin;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for schema admin api.
 *
 * <p>The tests run on the shared broker, each in a namespace of its own; {@link AdminApiSchemaOwnBrokerTest} has the
 * one that stubs the broker's BookKeeper client.
 */
@CustomLog
@Test(groups = "broker-admin")
public class AdminApiSchemaTest extends SharedPulsarBaseTest {

    enum ApiVersion{
        V2;
    }

    public static class Foo {
        int intField;
    }

    public static class Foo1 {
        int intField;

        String file1;
    }

    private static final Map<String, String> PROPS;

    static {
        PROPS = new HashMap<>();
        PROPS.put("key1", "value1");
    }

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @DataProvider(name = "schemas")
    public Object[][] schemas() {
        return new Object[][] {
            { Schema.BOOL },
            { Schema.INT8 },
            { Schema.INT16 },
            { Schema.INT32 },
            { Schema.INT64 },
            { StringSchema.utf8() },
            { new StringSchema(US_ASCII) },
            { Schema.FLOAT },
            { Schema.DOUBLE },
            { Schema.DATE },
            { Schema.TIME },
            { Schema.TIMESTAMP },
            { Schema.INSTANT },
            { Schema.LOCAL_DATE},
            { Schema.LOCAL_TIME},
            { Schema.LOCAL_DATE_TIME},
            { Schema.AVRO(
                SchemaDefinition.builder()
                    .withPojo(Foo.class)
                    .withProperties(PROPS)
                    .build()
            ) },
            { Schema.JSON(
                SchemaDefinition.builder()
                    .withPojo(Foo.class)
                    .withProperties(PROPS)
                    .build()
            )},
            { Schema.KeyValue(
                StringSchema.utf8(),
                new StringSchema(US_ASCII)
            )}
        };
    }

    @DataProvider(name = "version")
    public Object[][] versions() {
        return new Object[][] { { ApiVersion.V2 } };
    }

    @Test(dataProvider = "schemas")
    public void testSchemaInfoApi(Schema<?> schema) throws Exception {
        testSchemaInfoApi(schema, getNamespace() + "/test-" + schema.getSchemaInfo().getType());
    }

    @Test(dataProvider = "schemas")
    public void testSchemaInfoWithVersionApi(Schema<?> schema) throws Exception {
        testSchemaInfoWithVersionApi(schema, getNamespace() + "/test-" + schema.getSchemaInfo().getType());
    }

    private <T> void testSchemaInfoApi(Schema<T> schema,
                                       String topicName) throws Exception {
        SchemaInfo si = schema.getSchemaInfo();
        admin.schemas().createSchema(topicName, si);
        log.info().attr("topic", topicName).attr("schema", si).log("Upload schema to topic");

        SchemaInfo readSi = admin.schemas().getSchemaInfo(topicName);
        log.info().attr("topic", topicName).attr("schema", readSi).log("Read schema of topic");

        ((SchemaInfoImpl) readSi).setTimestamp(0);
        assertEquals(readSi, si);

        readSi = admin.schemas().getSchemaInfo(topicName + "-partition-0");
        log.info().attr("topic", topicName).attr("schema", readSi).log("Read schema of topic");

        ((SchemaInfoImpl) readSi).setTimestamp(0);
        assertEquals(readSi, si);

    }

    @SuppressWarnings("deprecation")
    @Test(dataProvider = "version")
    public void testPostSchemaCompatibilityStrategy(ApiVersion version) throws PulsarAdminException {
        String namespace = getNamespace();
        String topicName = "persistent://" + namespace + "/testStrategyChange";
        SchemaInfo fooSchemaInfo = Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false)
                .withPojo(Foo.class).build())
                .getSchemaInfo();

        admin.schemas().createSchema(topicName, fooSchemaInfo);
        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                SchemaAutoUpdateCompatibilityStrategy.Backward);
        SchemaInfo foo1SchemaInfo = Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false)
                .withPojo(Foo1.class).build())
                .getSchemaInfo();

        try {
            admin.schemas().createSchema(topicName, foo1SchemaInfo);
            fail("Should have failed");
        } catch (PulsarAdminException.ConflictException e) {
            assertTrue(e.getMessage().contains("HTTP 409"));
        }

        namespace = SharedPulsarCluster.TENANT_NAME + "/testnotfound";
        topicName = namespace + "/testStrategyChange";

        try {
            admin.schemas().createSchema(topicName, fooSchemaInfo);
            fail("Should have failed");
        } catch (PulsarAdminException.NotFoundException e) {
            assertTrue(e.getMessage().contains("Namespace does not exist"));
        }
    }

    private <T> void testSchemaInfoWithVersionApi(Schema<T> schema,
                                       String topicName) throws Exception {
        SchemaInfo si = schema.getSchemaInfo();
        admin.schemas().createSchema(topicName, si);
        log.info().attr("topic", topicName).attr("schema", si).log("Upload schema to topic");

        SchemaInfoWithVersion readSi = admin.schemas().getSchemaInfoWithVersion(topicName);
        log.info().attr("topic", topicName).attr("schema", readSi).log("Read schema of topic");

        ((SchemaInfoImpl) readSi.getSchemaInfo()).setTimestamp(0);
        assertEquals(readSi.getSchemaInfo(), si);
        assertEquals(readSi.getVersion(), 0);

        readSi = admin.schemas().getSchemaInfoWithVersion(topicName + "-partition-0");
        log.info().attr("topic", topicName).attr("schema", readSi).log("Read schema of topic");

        ((SchemaInfoImpl) readSi.getSchemaInfo()).setTimestamp(0);
        assertEquals(readSi.getSchemaInfo(), si);
        assertEquals(readSi.getVersion(), 0);

    }

    @Test(dataProvider = "version")
    public void createKeyValueSchema(ApiVersion version) throws Exception {
        String namespace = getNamespace();
        String topicName = "persistent://" + namespace + "/test-key-value-schema";
        Schema<?> keyValueSchema = Schema.KeyValue(Schema.AVRO(Foo.class), Schema.AVRO(Foo.class));
        admin.schemas().createSchema(topicName, keyValueSchema.getSchemaInfo());
        SchemaInfo schemaInfo = admin.schemas().getSchemaInfo(topicName);

        long timestamp = schemaInfo.getTimestamp();
        assertNotEquals(keyValueSchema.getSchemaInfo().getTimestamp(), timestamp);
        assertNotEquals(0, timestamp);

        ((SchemaInfoImpl) keyValueSchema.getSchemaInfo()).setTimestamp(schemaInfo.getTimestamp());
        assertEquals(keyValueSchema.getSchemaInfo(), schemaInfo);

        admin.schemas().createSchema(topicName, keyValueSchema.getSchemaInfo());
        SchemaInfo schemaInfo2 = admin.schemas().getSchemaInfo(topicName);
        assertEquals(timestamp, schemaInfo2.getTimestamp());
    }


    @Test(dataProvider = "version")
    public void testInvalidSchemaDataException(ApiVersion version) {
        String namespace = getNamespace();
        String topicName = "persistent://" + namespace + "/test-invalid-schema-data-exception";
        SchemaInfo schemaInfo = SchemaInfo.builder()
                .schema(new byte[0])
                .type(SchemaType.AVRO)
                .name("test")
                .build();
        try {
            admin.schemas().createSchema(topicName, schemaInfo);
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertTrue(e.getMessage().contains("Invalid schema definition data for AVRO schema"));
        }
    }

    @Test
    public void testGetSchemaCompatibilityStrategy() throws PulsarAdminException {
        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                SchemaCompatibilityStrategy.UNDEFINED);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetSchemaAutoUpdateCompatibilityStrategy() throws PulsarAdminException {
        assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(getNamespace()));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetSchemaCompatibilityStrategyWhenSetSchemaAutoUpdateCompatibilityStrategy()
            throws PulsarAdminException {
        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                SchemaCompatibilityStrategy.UNDEFINED);

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(getNamespace(),
                SchemaAutoUpdateCompatibilityStrategy.Forward);
        Awaitility.await().untilAsserted(() -> assertEquals(SchemaAutoUpdateCompatibilityStrategy.Forward,
                admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(getNamespace())
        ));

        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                SchemaCompatibilityStrategy.UNDEFINED);

        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.BACKWARD);
        Awaitility.await().untilAsserted(() -> assertEquals(SchemaCompatibilityStrategy.BACKWARD,
                admin.namespaces().getSchemaCompatibilityStrategy(getNamespace())));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetSchemaCompatibilityStrategyWhenSetBrokerLevelAndSchemaAutoUpdateCompatibilityStrategy()
            throws PulsarAdminException {
        // the shared broker reads this setting when a topic loads, and this test loads none
        SchemaCompatibilityStrategy brokerStrategy = getConfig().getSchemaCompatibilityStrategy();
        getConfig().setSchemaCompatibilityStrategy(SchemaCompatibilityStrategy.FORWARD);
        try {
            assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                    SchemaCompatibilityStrategy.UNDEFINED);

            admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(getNamespace(),
                    SchemaAutoUpdateCompatibilityStrategy.AlwaysCompatible);
            Awaitility.await().untilAsserted(() -> assertEquals(
                    admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                    SchemaCompatibilityStrategy.UNDEFINED));
        } finally {
            getConfig().setSchemaCompatibilityStrategy(brokerStrategy);
        }
    }

    @Test
    public void testCompatibility() throws Exception {
        String topicName = getNamespace() + "/testCompatibility";
        try {
            admin.schemas().getSchemaInfo(topicName);
            fail();
        } catch (PulsarAdminException.NotFoundException e) {
            assertEquals(e.getMessage(), "Schema not found");
        }
        Map<String, String> properties = new HashMap<>();
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload("STRING", "", properties);
        admin.schemas().createSchema(topicName, postSchemaPayload);
        IsCompatibilityResponse isCompatibilityResponse =
                admin.schemas().testCompatibility(topicName, postSchemaPayload);

        assertTrue(isCompatibilityResponse.isCompatibility());
        assertEquals(isCompatibilityResponse.getSchemaCompatibilityStrategy(), SchemaCompatibilityStrategy.FULL.name());
        postSchemaPayload = new PostSchemaPayload("INT8", "", properties);
        try {
            admin.schemas().testCompatibility(topicName, postSchemaPayload);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof PulsarAdminException.ServerSideErrorException);
            assertTrue(e.getMessage().contains("Incompatible schema: exists schema type STRING, new schema type INT8"));
        }
    }

    @Test
    public void testCompatibilityWithEmpty() throws Exception {
        List<Schema<?>> checkSchemas = List.of(
                Schema.STRING,
                Schema.JSON(SchemaDefinition.builder().withPojo(Foo.class).withProperties(PROPS).build()),
                Schema.AVRO(SchemaDefinition.builder().withPojo(Foo.class).withProperties(PROPS).build()),
                Schema.KeyValue(Schema.STRING, Schema.STRING)
        );
        for (Schema<?> schema : checkSchemas) {
            SchemaInfo schemaInfo = schema.getSchemaInfo();
            String topicName = getNamespace() + "/testCompatibilityWithEmpty";
            PostSchemaPayload postSchemaPayload = new PostSchemaPayload(schemaInfo.getType().toString(),
                    schemaInfo.getSchemaDefinition(), new HashMap<>());

            // check compatibility with empty schema
            IsCompatibilityResponse isCompatibilityResponse =
                    admin.schemas().testCompatibility(topicName, postSchemaPayload);
            assertTrue(isCompatibilityResponse.isCompatibility());
            assertEquals(isCompatibilityResponse.getSchemaCompatibilityStrategy(),
                    SchemaCompatibilityStrategy.FULL.name());

            // set schema compatibility strategy is FULL_TRANSITIVE to cover checkCompatibilityWithAll
            admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                    SchemaCompatibilityStrategy.FULL_TRANSITIVE);
            isCompatibilityResponse = admin.schemas().testCompatibility(topicName, postSchemaPayload);
            assertTrue(isCompatibilityResponse.isCompatibility());
            assertEquals(isCompatibilityResponse.getSchemaCompatibilityStrategy(),
                    SchemaCompatibilityStrategy.FULL_TRANSITIVE.name());
            // set back to FULL
            admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                    SchemaCompatibilityStrategy.FULL);
        }
    }
}
