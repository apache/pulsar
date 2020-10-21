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
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.util.Map;

import static org.testng.Assert.*;

@Slf4j
public class AdminApiMaxUnackedMessages extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("max-unacked-messages", tenantInfo);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMaxUnackedMessagesOnConsumers() throws Exception {
        admin.namespaces().createNamespace("max-unacked-messages/default-on-consumers");
        String namespace = "max-unacked-messages/default-on-consumers";
        assertEquals(50000, admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 2*50000);
        assertEquals(2*50000, admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
    }

    @Test
    public void testMaxUnackedMessagesOnSubscription() throws Exception {
        admin.namespaces().createNamespace("max-unacked-messages/default-on-subscription");
        String namespace = "max-unacked-messages/default-on-subscription";
        assertEquals(200000, admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
        admin.namespaces().setMaxUnackedMessagesPerSubscription(namespace, 2*200000);
        assertEquals(2*200000, admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
    }
}
