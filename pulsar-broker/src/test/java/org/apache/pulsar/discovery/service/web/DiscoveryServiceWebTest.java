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
package org.apache.pulsar.discovery.service.web;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.discovery.service.server.ServerManager;
import org.apache.pulsar.discovery.service.server.ServiceConfig;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.zookeeper.ZooKeeper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DiscoveryServiceWebTest extends ProducerConsumerBase {

    private Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * 1. Start : Broker and Discovery service. 2. Provide started broker server as active broker to Discovery service
     * 3. Call GET, PUT, POST request to discovery service that redirects to Broker service and receives response
     *
     * @throws Exception
     */
    @Test
    public void testRedirectUrlWithServerStarted() throws Exception {
        // 1. start server
        ServiceConfig config = new ServiceConfig();
        config.setWebServicePort(Optional.of(0));
        ServerManager server = new ServerManager(config);
        DiscoveryZooKeeperClientFactoryImpl.zk = mockZooKeeper;
        Map<String, String> params = new TreeMap<>();
        params.put("zookeeperServers", "");
        params.put("zookeeperClientFactoryClass", DiscoveryZooKeeperClientFactoryImpl.class.getName());
        server.addServlet("/", DiscoveryServiceServlet.class, params);
        server.start();

        String serviceUrl = server.getServiceUri().toString();
        String putRequestUrl = serviceUrl + "admin/v2/namespaces/p1/n1";
        String postRequestUrl = serviceUrl + "admin/v2/namespaces/p1/n1/replication";
        String getRequestUrl = serviceUrl + "admin/v2/namespaces/p1";

        /**
         * verify : every time when vip receives a request: it redirects to above brokers sequentially and broker
         * returns appropriate response which must not be null.
         **/

        assertEquals(hitBrokerService(HttpMethod.POST, postRequestUrl, Lists.newArrayList("use")),
                "Tenant does not exist");
        assertEquals(hitBrokerService(HttpMethod.PUT, putRequestUrl, new BundlesData(1)), "Tenant does not exist");
        assertEquals(hitBrokerService(HttpMethod.GET, getRequestUrl, null), "Tenant does not exist");

        server.stop();

    }

    public String hitBrokerService(String method, String url, Object data) throws JsonParseException {

        Response response = null;
        try {
            WebTarget webTarget = client.target(url);
            Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
            if (HttpMethod.PUT.equals(method)) {
                response = (Response) invocationBuilder.put(Entity.entity(data, MediaType.APPLICATION_JSON));
            } else if (HttpMethod.GET.equals(method)) {
                response = (Response) invocationBuilder.get();
            } else if (HttpMethod.POST.equals(method)) {
                response = (Response) invocationBuilder.post(Entity.entity(data, MediaType.APPLICATION_JSON));
            } else {
                fail("Unsupported http method");
            }
        } catch (Exception e) {
            // fail
            fail();
        }

        JsonObject jsonObject = new Gson().fromJson(response.readEntity(String.class), JsonObject.class);
        String serviceResponse = jsonObject.get("reason").getAsString();
        return serviceResponse;
    }

    static class DiscoveryZooKeeperClientFactoryImpl implements ZooKeeperClientFactory {
        static ZooKeeper zk;

        @Override
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                int zkSessionTimeoutMillis) {
            return CompletableFuture.completedFuture(zk);
        }
    }

}
