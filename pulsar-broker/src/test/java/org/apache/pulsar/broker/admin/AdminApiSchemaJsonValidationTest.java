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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import java.util.HashMap;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for PIP-464: Strict Avro schema validation for SchemaType.JSON.
 * Tests that the broker correctly accepts/rejects schema definitions based on
 * the schemaJsonAllowLegacyJacksonFormat configuration.
 */
@Slf4j
@Test(groups = "broker-admin")
public class AdminApiSchemaJsonValidationTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("schema-json-validation", tenantInfo);
        admin.namespaces().createNamespace("schema-json-validation/test-ns", Set.of("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAvroFormatJsonSchemaAccepted() throws Exception {
        // Valid Avro schema for SchemaType.JSON should always be accepted
        String topicName = "persistent://schema-json-validation/test-ns/avro-format-accepted";
        String avroSchema = "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"org.example\","
                + "\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},"
                + "{\"name\":\"field2\",\"type\":\"int\"}]}";

        PostSchemaPayload payload = new PostSchemaPayload("JSON", avroSchema, new HashMap<>());
        admin.schemas().createSchema(topicName, payload);

        // Verify the schema was stored
        assertNotNull(admin.schemas().getSchemaInfo(topicName));
        assertEquals(admin.schemas().getSchemaInfo(topicName).getType().name(), "JSON");
    }

    @Test
    public void testAvroFormatJsonSchemaViaProducerAccepted() throws Exception {
        // Java client's JSONSchema.of() generates Avro format — should always be accepted
        String topicName = "persistent://schema-json-validation/test-ns/avro-format-producer-accepted";

        try (var producer = pulsarClient.newProducer(
                Schema.JSON(SchemaDefinition.builder().withPojo(TestPojo.class).build()))
                .topic(topicName).create()) {
            producer.send(new TestPojo("hello", 42));
        }

        // Verify the schema was stored
        assertNotNull(admin.schemas().getSchemaInfo(topicName));
        assertEquals(admin.schemas().getSchemaInfo(topicName).getType().name(), "JSON");
    }

    @Test
    public void testJsonSchemaDraftRejectedByDefault() throws Exception {
        // JSON Schema Draft 2020-12 (the problematic case from non-Java clients) should be rejected
        // when schemaJsonAllowLegacyJacksonFormat=false (default)
        String topicName = "persistent://schema-json-validation/test-ns/json-draft-rejected";
        String jsonSchemaDraft = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
                + "\"type\":\"object\",\"properties\":{\"field1\":{\"type\":\"string\"},"
                + "\"field2\":{\"type\":\"integer\"}}}";

        PostSchemaPayload payload = new PostSchemaPayload("JSON", jsonSchemaDraft, new HashMap<>());
        try {
            admin.schemas().createSchema(topicName, payload);
            fail("Should reject JSON Schema Draft format when schemaJsonAllowLegacyJacksonFormat=false");
        } catch (PulsarAdminException e) {
            log.info("Expected rejection: {}", e.getMessage());
        }
    }

    @Test
    public void testJacksonJsonSchemaRejectedByDefault() throws Exception {
        // Old Jackson JsonSchema format should be rejected when schemaJsonAllowLegacyJacksonFormat=false (default)
        String topicName = "persistent://schema-json-validation/test-ns/jackson-rejected";
        String jacksonSchema = "{\"type\":\"object\",\"id\":\"urn:jsonschema:org:example:TestRecord\","
                + "\"properties\":{\"field1\":{\"type\":\"string\"},\"field2\":{\"type\":\"integer\"}}}";

        PostSchemaPayload payload = new PostSchemaPayload("JSON", jacksonSchema, new HashMap<>());
        try {
            admin.schemas().createSchema(topicName, payload);
            fail("Should reject Jackson JsonSchema format when schemaJsonAllowLegacyJacksonFormat=false");
        } catch (PulsarAdminException e) {
            log.info("Expected rejection: {}", e.getMessage());
        }
    }

    @Test
    public void testJacksonJsonSchemaAcceptedWhenLegacyEnabled() throws Exception {
        // Restart broker with legacy format enabled
        cleanup();
        conf.setSchemaJsonAllowLegacyJacksonFormat(true);
        setup();

        String topicName = "persistent://schema-json-validation/test-ns/jackson-accepted-legacy";
        String jacksonSchema = "{\"type\":\"object\",\"id\":\"urn:jsonschema:org:example:TestRecord\","
                + "\"properties\":{\"field1\":{\"type\":\"string\"},\"field2\":{\"type\":\"integer\"}}}";

        PostSchemaPayload payload = new PostSchemaPayload("JSON", jacksonSchema, new HashMap<>());
        admin.schemas().createSchema(topicName, payload);

        // Verify the schema was stored
        assertNotNull(admin.schemas().getSchemaInfo(topicName));
        assertEquals(admin.schemas().getSchemaInfo(topicName).getType().name(), "JSON");
    }

    @Test
    public void testJsonSchemaDraftAcceptedWhenLegacyEnabled() throws Exception {
        // Restart broker with legacy format enabled
        cleanup();
        conf.setSchemaJsonAllowLegacyJacksonFormat(true);
        setup();

        String topicName = "persistent://schema-json-validation/test-ns/json-draft-accepted-legacy";
        String jsonSchemaDraft = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
                + "\"type\":\"object\",\"properties\":{\"field1\":{\"type\":\"string\"},"
                + "\"field2\":{\"type\":\"integer\"}}}";

        PostSchemaPayload payload = new PostSchemaPayload("JSON", jsonSchemaDraft, new HashMap<>());
        admin.schemas().createSchema(topicName, payload);

        // Verify the schema was stored
        assertNotNull(admin.schemas().getSchemaInfo(topicName));
    }

    @Test
    public void testAvroSchemaTypeUnaffectedByConfig() throws Exception {
        // SchemaType.AVRO should always require Avro format, regardless of the JSON legacy config
        String topicName = "persistent://schema-json-validation/test-ns/avro-type-unaffected";
        String avroSchema = "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"org.example\","
                + "\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";

        PostSchemaPayload payload = new PostSchemaPayload("AVRO", avroSchema, new HashMap<>());
        admin.schemas().createSchema(topicName, payload);

        // Verify the schema was stored
        assertNotNull(admin.schemas().getSchemaInfo(topicName));
        assertEquals(admin.schemas().getSchemaInfo(topicName).getType().name(), "AVRO");
    }

    @Test
    public void testSchemaCompatibilityRejectsNonAvroByDefault() throws Exception {
        // First register a valid Avro schema
        String topicName = "persistent://schema-json-validation/test-ns/compat-rejects-non-avro";
        String avroSchemaV1 = "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"org.example\","
                + "\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";

        PostSchemaPayload payload1 = new PostSchemaPayload("JSON", avroSchemaV1, new HashMap<>());
        admin.schemas().createSchema(topicName, payload1);

        // Now try to register a JSON Schema Draft as v2 — should be rejected
        String jsonSchemaDraft = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
                + "\"type\":\"object\",\"properties\":{\"field1\":{\"type\":\"string\"}}}";

        PostSchemaPayload payload2 = new PostSchemaPayload("JSON", jsonSchemaDraft, new HashMap<>());
        try {
            admin.schemas().createSchema(topicName, payload2);
            fail("Should reject JSON Schema Draft as incompatible with existing Avro schema");
        } catch (PulsarAdminException e) {
            log.info("Expected rejection on compatibility check: {}", e.getMessage());
        }
    }

    private static class TestPojo {
        public String field1;
        public int field2;

        public TestPojo() {}

        public TestPojo(String field1, int field2) {
            this.field1 = field1;
            this.field2 = field2;
        }
    }
}
