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

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.testng.Assert;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.util.Map;

@Slf4j
public class AdminApiSchemaValidationEnforced extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiSchemaValidationEnforced.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("schema-validation-enforced", tenantInfo);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDisableSchemaValidationEnforcedNoSchema() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/default-no-schema");
        String namespace = "schema-validation-enforced/default-no-schema";
        String topicName = "persistent://schema-validation-enforced/default-no-schema/test";
        Assert.assertEquals(admin.namespaces().getSchemaValidationEnforced(namespace), false);
        admin.namespaces().setSchemaValidationEnforced(namespace, false);
        try {
            admin.schemas().getSchemaInfo(topicName);
        } catch (PulsarAdminException.NotFoundException e) {
            Assert.assertTrue(e.getMessage().contains("HTTP 404 Not Found"));
        }
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            p.send("test schemaValidationEnforced".getBytes());
        }
    }

    @Test
    public void testDisableSchemaValidationEnforcedHasSchema() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/default-has-schema");
        String namespace = "schema-validation-enforced/default-has-schema";
        String topicName = "persistent://schema-validation-enforced/default-has-schema/test";
        Assert.assertEquals(admin.namespaces().getSchemaValidationEnforced(namespace), false);
        admin.namespaces().setSchemaValidationEnforced(namespace, false);
        try {
            admin.schemas().getSchemaInfo(topicName);
        } catch (PulsarAdminException.NotFoundException e) {
            Assert.assertTrue(e.getMessage().contains("HTTP 404 Not Found"));
        }
        Map<String, String> properties = Maps.newHashMap();
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setType(SchemaType.STRING);
        schemaInfo.setProperties(properties);
        schemaInfo.setName("test");
        schemaInfo.setSchema("".getBytes());
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload("STRING", "", properties);
        admin.schemas().createSchema(topicName, postSchemaPayload);
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            p.send("test schemaValidationEnforced".getBytes());
        }
        Assert.assertEquals(admin.schemas().getSchemaInfo(topicName), schemaInfo);
    }


    @Test
    public void testEnableSchemaValidationEnforcedNoSchema() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/enable-no-schema");
        String namespace = "schema-validation-enforced/enable-no-schema";
        String topicName = "persistent://schema-validation-enforced/enable-no-schema/test";
        Assert.assertEquals(admin.namespaces().getSchemaValidationEnforced(namespace), false);
        admin.namespaces().setSchemaValidationEnforced(namespace,true);
        try {
            admin.schemas().getSchemaInfo(topicName);
        } catch (PulsarAdminException.NotFoundException e) {
            Assert.assertTrue(e.getMessage().contains("HTTP 404 Not Found"));
        }
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            p.send("test schemaValidationEnforced".getBytes());
        }
    }

    @Test
    public void testEnableSchemaValidationEnforcedHasSchemaMismatch() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/enable-has-schema-mismatch");
        String namespace = "schema-validation-enforced/enable-has-schema-mismatch";
        String topicName = "persistent://schema-validation-enforced/enable-has-schema-mismatch/test";
        Assert.assertEquals(admin.namespaces().getSchemaValidationEnforced(namespace), false);
        admin.namespaces().setSchemaValidationEnforced(namespace,true);
        Assert.assertEquals(admin.namespaces().getSchemaValidationEnforced(namespace), true);
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().getStats(topicName);
        try {
            admin.schemas().getSchemaInfo(topicName);
        } catch (PulsarAdminException.NotFoundException e) {
            Assert.assertTrue(e.getMessage().contains("HTTP 404 Not Found"));
        }
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key1", "value1");
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setType(SchemaType.STRING);
        schemaInfo.setProperties(properties);
        schemaInfo.setName("test");
        schemaInfo.setSchema("".getBytes());
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload("STRING", "{'key':'value'}", properties);
        admin.schemas().createSchema(topicName, postSchemaPayload);
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            Assert.fail("Client no schema, but topic has schema, should fail");
        }  catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }
        Assert.assertEquals(admin.schemas().getSchemaInfo(topicName).getName(), schemaInfo.getName());
        Assert.assertEquals(admin.schemas().getSchemaInfo(topicName).getType(), schemaInfo.getType());
    }

    @Test
    public void testEnableSchemaValidationEnforcedHasSchemaMatch() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/enable-has-schema-match");
        String namespace = "schema-validation-enforced/enable-has-schema-match";
        String topicName = "persistent://schema-validation-enforced/enable-has-schema-match/test";
        Assert.assertEquals(admin.namespaces().getSchemaValidationEnforced(namespace), false);
        try {
            admin.schemas().getSchemaInfo(topicName);
        } catch (PulsarAdminException.NotFoundException e) {
            Assert.assertTrue(e.getMessage().contains("HTTP 404 Not Found"));
        }
        admin.namespaces().setSchemaValidationEnforced(namespace,true);
        Map<String, String> properties = Maps.newHashMap();
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setType(SchemaType.STRING);
        schemaInfo.setProperties(properties);
        schemaInfo.setName("test");
        schemaInfo.setSchema("".getBytes());
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload("STRING", "", properties);
        admin.schemas().createSchema(topicName, postSchemaPayload);
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
            p.send("test schemaValidationEnforced");
        }
        Assert.assertEquals(admin.schemas().getSchemaInfo(topicName).getName(), schemaInfo.getName());
        Assert.assertEquals(admin.schemas().getSchemaInfo(topicName).getType(), schemaInfo.getType());
    }

}
