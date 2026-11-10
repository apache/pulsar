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
import java.util.HashMap;
import java.util.Set;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * PIP-464 tests for a broker with schemaJsonAllowLegacyJacksonFormat=true, a setting the broker reads when it
 * starts, so they run on a broker of their own. {@link AdminApiSchemaJsonValidationTest} has the tests for the
 * default configuration.
 */
@Test(groups = "broker-admin")
public class AdminApiSchemaJsonValidationLegacyFormatTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setSchemaJsonAllowLegacyJacksonFormat(true);
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("schema-json-validation", tenantInfo);
        admin.namespaces().createNamespace("schema-json-validation/test-ns", Set.of("test"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testJacksonJsonSchemaAcceptedWhenLegacyEnabled() throws Exception {
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
        String topicName = "persistent://schema-json-validation/test-ns/json-draft-accepted-legacy";
        String jsonSchemaDraft = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
                + "\"type\":\"object\",\"properties\":{\"field1\":{\"type\":\"string\"},"
                + "\"field2\":{\"type\":\"integer\"}}}";

        PostSchemaPayload payload = new PostSchemaPayload("JSON", jsonSchemaDraft, new HashMap<>());
        admin.schemas().createSchema(topicName, payload);

        // Verify the schema was stored
        assertNotNull(admin.schemas().getSchemaInfo(topicName));
    }
}
