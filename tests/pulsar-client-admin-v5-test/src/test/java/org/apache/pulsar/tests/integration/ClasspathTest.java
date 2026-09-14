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
package org.apache.pulsar.tests.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Collections;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.testng.annotations.Test;

public class ClasspathTest {
    @Test
    public void implementationsAreIsolated() throws Exception {
        ClassLoader loader = getClass().getClassLoader();
        assertEquals(Collections.list(loader.getResources("org/apache/pulsar/client/impl/ClientCnx.class")).size(), 1);
        assertNotNull(loader.getResource("org/apache/pulsar/shade/org/apache/pulsar/client/impl/ClientCnx.class"));
        assertEquals(Collections.list(loader.getResources(
                "com/scurrilous/circe/checksum/Crc32cIntChecksum.class")).size(), 1);
        try (PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
             PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build()) {
            assertNotNull(client);
            assertNotNull(admin);
            assertEquals(Schema.string().decode(Schema.string().encode("hello")), "hello");
            assertEquals(ClusterData.builder().serviceUrl("http://localhost:8080").build().getServiceUrl(),
                    "http://localhost:8080");
            assertEquals(OffloadProcessStatus.forSuccess(MessageId.earliest)
                    .getFirstUnoffloadedMessage(), MessageId.earliest);
        }
    }
}
