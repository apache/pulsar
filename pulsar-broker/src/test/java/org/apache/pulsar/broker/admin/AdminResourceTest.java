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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.util.Codec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test {@link AdminResource}.
 */
@Test(groups = "broker")
public class AdminResourceTest extends BrokerTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static AdminResource mockResource() {
        return new AdminResource() {

            @Override
            protected String domain() {
                return "persistent";
            }
        };
    }

    private static AdminResource mockNonPersistentResource() {
        return new AdminResource() {

            @Override
            protected String domain() {
                return "non-persistent";
            }
        };
    }

    @Test
    public void testValidatePersistentTopicNameSuccess() {
        String tenant = "test-tenant";
        String namespace = "test-namespace";
        String topic = Codec.encode("test-topic");

        AdminResource resource = mockResource();
        resource.validatePersistentTopicName(tenant, namespace, topic);
    }

    @Test
    public void testValidatePersistentTopicNameInvalid() {
        String tenant = "test-tenant";
        String namespace = "test-namespace";
        String topic = Codec.encode("test-topic");

        AdminResource nPResource = mockNonPersistentResource();
        try {
            nPResource.validatePersistentTopicName(tenant, namespace, topic);
            fail("Should fail validation on non-persistent topic");
        } catch (RestException e) {
            assertEquals(Status.NOT_ACCEPTABLE.getStatusCode(), e.getResponse().getStatus());
        }
    }

    @Test
    public void testValidatePartitionedTopicNameSuccess() {
        String tenant = "test-tenant";
        String namespace = "test-namespace";
        String topic = Codec.encode("test-topic");

        AdminResource resource = mockResource();
        resource.validatePartitionedTopicName(tenant, namespace, topic);
    }

    @Test
    public void testValidatePartitionedTopicNameInvalid() {
        String tenant = "test-tenant";
        String namespace = "test-namespace";
        String topic = Codec.encode("test-topic-partition-0");

        AdminResource resource = mockResource();
        try {
            resource.validatePartitionedTopicName(tenant, namespace, topic);
            fail("Should fail validation on invalid partitioned topic");
        } catch (RestException re) {
            assertEquals(Status.PRECONDITION_FAILED.getStatusCode(), re.getResponse().getStatus());
        }
    }

    @Test
    public void testValidatePartitionedTopicMetadata() throws Exception {
        String tenant = "prop";
        String namespace = "ns-abc";
        String partitionedTopic = "partitionedTopic";
        String nonPartitionedTopic = "notPartitionedTopic";
        int partitions = 3;

        String completePartitionedTopic = tenant + "/" + namespace + "/" + partitionedTopic;
        String completeNonPartitionedTopic = tenant + "/" + namespace + "/" + nonPartitionedTopic;

        admin.topics().createNonPartitionedTopic(completeNonPartitionedTopic);
        admin.topics().createPartitionedTopic(completePartitionedTopic, partitions);

        AdminResource resource = mockResource();
        resource.setPulsar(pulsar);
        // validate should pass when topic is partitioned topic
        resource.validatePartitionedTopicName(tenant, namespace, Codec.encode(partitionedTopic));
        resource.validatePartitionedTopicMetadata(tenant, namespace, Codec.encode(partitionedTopic));
        // validate should failed when topic is non-partitioned topic
        resource.validatePartitionedTopicName(tenant, namespace, Codec.encode(nonPartitionedTopic));
        try {
            resource.validatePartitionedTopicMetadata(tenant, namespace, Codec.encode(nonPartitionedTopic));
            fail("Should fail validation on non-partitioned topic");
        } catch (RestException re) {
            assertEquals(Status.CONFLICT.getStatusCode(), re.getResponse().getStatus());
        }
    }
}
