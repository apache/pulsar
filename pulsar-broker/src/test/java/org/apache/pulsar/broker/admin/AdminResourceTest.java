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
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.util.Codec;
import org.testng.annotations.Test;

/**
 * Unit test {@link AdminResource}.
 */
public class AdminResourceTest {

    private static AdminResource mockResource() {
        return new AdminResource() {

            @Override
            protected String domain() {
                return "persistent";
            }
        };
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

}
