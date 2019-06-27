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
package org.apache.pulsar.broker.admin;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for schema admin api.
 */
@Slf4j
public class AdminApiSchemaTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", new ClusterData("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("schematest", tenantInfo);
        admin.namespaces().createNamespace("schematest/test", Sets.newHashSet("test"));
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    public static class Foo {
        int intField;
    }

    private static Map<String, String> PROPS;

    static {
        PROPS = new HashMap<>();
        PROPS.put("key1", "value1");
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

    @Test(dataProvider = "schemas")
    public void testSchemaInfoApi(Schema<?> schema) throws Exception {
        testSchemaInfoApi(schema, "schematest/test/test-" + schema.getSchemaInfo().getType());
    }

    private <T> void testSchemaInfoApi(Schema<T> schema,
                                       String topicName) throws Exception {
        SchemaInfo si = schema.getSchemaInfo();
        admin.schemas().createSchema(topicName, si);
        log.info("Upload schema to topic {} : {}", topicName, si);

        SchemaInfo readSi = admin.schemas().getSchemaInfo(topicName);
        log.info("Read schema of topic {} : {}", topicName, readSi);

        assertEquals(si, readSi);
    }
}
