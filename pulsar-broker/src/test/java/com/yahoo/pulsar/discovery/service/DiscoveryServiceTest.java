/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.discovery.service;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.bookkeeper.test.PortManager;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.pulsar.client.api.ProducerConsumerBase;
import com.yahoo.pulsar.common.policies.data.BundlesData;
import com.yahoo.pulsar.discovery.service.server.ServerManager;
import com.yahoo.pulsar.discovery.service.server.ServiceConfig;

public class DiscoveryServiceTest extends ProducerConsumerBase {

    private Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));

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
    public void testRiderectUrlWithServerStarted() throws Exception {
        // 1. start server
        int port = PortManager.nextFreePort();
        ServiceConfig config = new ServiceConfig();
        config.setWebServicePort(port);
        ServerManager server = new ServerManager(config);

        Map<String, String> params = new TreeMap<>();
        params.put("zookeeperServers", "dummy-value");
        params.put("zooKeeperSessionTimeoutMillis", "1000");
        server.addServlet("/", DiscoveryServiceServlet.class, params);
        server.start();

        ZookeeperCacheLoader.availableActiveBrokers.add(super.brokerUrl.getHost() + ":" + super.brokerUrl.getPort());

        Thread.sleep(200);

        String serviceUrl = server.getServiceUri().toString();
        String putRequestUrl = serviceUrl + "admin/namespaces/p1/c1/n1";
        String postRequestUrl = serviceUrl + "admin/namespaces/p1/c1/n1/permissions/test-role";
        String getRequestUrl = serviceUrl + "admin/namespaces/p1";

        /**
         * verify : every time when vip receives a request: it redirects to above brokers sequentially and broker
         * returns appropriate response which must not be null.
         **/
        assertNotNull(hitBrokerService(HttpMethod.POST, postRequestUrl, null));
        assertNotNull(hitBrokerService(HttpMethod.PUT, putRequestUrl, new BundlesData(1)));
        assertNotNull(hitBrokerService(HttpMethod.GET, getRequestUrl, null));

        server.stop();

    }

    public String hitBrokerService(String method, String url, BundlesData bundle) throws JSONException {

        Response response = null;
        try {
            WebTarget webTarget = client.target(url);
            Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
            if (HttpMethod.PUT.equals(method)) {
                response = (Response) invocationBuilder.put(Entity.entity(bundle, MediaType.APPLICATION_JSON));
            } else if (HttpMethod.GET.equals(method)) {
                response = (Response) invocationBuilder.get();
            } else if (HttpMethod.POST.equals(method)) {
                response = (Response) invocationBuilder.post(Entity.entity(bundle, MediaType.APPLICATION_JSON));
            } else {
                fail("Unsupported http method");
            }
        } catch (Exception e) {
            // fail
            fail();
        }

        String s = response.readEntity(String.class);
        JSONObject jsonObject = new JSONObject();
        String serviceResponse = jsonObject.getString("reason");
        return serviceResponse;
    }

}
