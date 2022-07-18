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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin-v2")
public class AdminRestTest extends MockedPulsarServiceBaseTest {

    private final String clusterName = "test";
    private final String tenantName = "t-tenant";
    private final String namespaceName = "t-tenant/test-namespace";
    private final String topicNameSuffix = "t-rest-topic";
    private final String topicName = "persistent://" + namespaceName + "/" + topicNameSuffix;

    @Test
    public void testRejectUnknownEntityProperties() throws Exception{
        // Build request command.
        int port = pulsar.getWebService().getListenPortHTTP().get();
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target("http://127.0.0.1:" + port
                + "/admin/v2/persistent/" + namespaceName + "/" + topicNameSuffix + "/retention");
        Map<String,Object> data = new HashMap<>();
        data.put("retention_size_in_mb", -1);
        data.put("retention_time_in_minutes", 40320);
        // Configuration default, response success.
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).buildPost(Entity.json(data)).invoke();
        Assert.assertTrue(response.getStatus() / 200 == 1);
        // Enabled feature, bad request response.
        pulsar.getWebService().getSharedUnknownPropertyHandler().setSkipUnknownProperty(false);
        response = target.request(MediaType.APPLICATION_JSON_TYPE).buildPost(Entity.json(data)).invoke();
        Assert.assertEquals(response.getStatus(), 400);
        // Disabled feature, response success.
        pulsar.getWebService().getSharedUnknownPropertyHandler().setSkipUnknownProperty(true);
        response = target.request(MediaType.APPLICATION_JSON_TYPE).buildPost(Entity.json(data)).invoke();
        Assert.assertTrue(response.getStatus() / 200 == 1);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        resetConfig();
        super.internalSetup();
        // Create tenant, namespace, topic
        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant(tenantName,
                new TenantInfoImpl(Collections.singleton("a"), Collections.singleton(clusterName)));
        admin.namespaces().createNamespace(namespaceName);
        admin.topics().createNonPartitionedTopic(topicName);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        // cleanup.
        admin.topics().delete(topicName);
        admin.namespaces().deleteNamespace(namespaceName);
        admin.tenants().deleteTenant(tenantName);
        admin.clusters().deleteCluster(clusterName);
        // super cleanup.
        super.internalCleanup();
    }
}
