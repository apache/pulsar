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

import static javax.ws.rs.core.Response.Status.BAD_GATEWAY;
import static org.apache.pulsar.broker.resources.MetadataStoreCacheLoader.LOADBALANCE_BROKERS_ROOT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.broker.resources.MetadataStoreCacheLoader;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.discovery.service.server.ServerManager;
import org.apache.pulsar.discovery.service.server.ServiceConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * 1. starts discovery service a. loads broker list from zk 2. http-client calls multiple http request: GET, PUT and
 * POST. 3. discovery service redirects to appropriate brokers in round-robin 4. client receives unknown host exception
 * with redirected broker
 *
 */
public class DiscoveryServiceWebTest extends BaseZKStarterTest{

    private static final Logger log = LoggerFactory.getLogger(DiscoveryServiceWebTest.class);

    private Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
    private static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    // DiscoveryServiceServlet gets initialized by a server and this map will help to retrieve ZK while mocking
    // DiscoveryServiceServlet
    private static final Map<String, MetadataStoreExtended> metadataStoreInstanceCache = Maps.newConcurrentMap();

    @BeforeMethod
    private void init() throws Exception {
        start();
    }

    @AfterMethod(alwaysRun = true)
    private void cleanup() throws Exception {
        close();
        metadataStoreInstanceCache.clear();
    }

    @Test
    public void testNextBroker() throws Exception {

        PulsarResources resources = new PulsarResources(zkStore, null);

        // 1. create znode for each broker
        List<String> brokers = Lists.newArrayList("broker-1", "broker-2", "broker-3");
        brokers.stream().forEach(broker -> {
            String path = LOADBALANCE_BROKERS_ROOT + "/" + broker;
            try {
                LoadReport report = new LoadReport(broker, null, null, null);
                String reportData = ObjectMapperFactory.getThreadLocal().writeValueAsString(report);
                zkStore.put(path, reportData.getBytes(StandardCharsets.UTF_8), Optional.of(-1L))
                        .get();
            } catch (ExecutionException ne) {
                // Ok
            } catch (Exception e) {
                if (e instanceof ExecutionException && (e.getCause()) instanceof AlreadyExistsException) {
                    // Ok
                } else {
                    log.warn("Failed to write to metadata-store {}", path, e);
                    fail("failed while creating broker znodes");
                }
            }
        });

        // 2. Setup discovery-zkcache
        DiscoveryServiceServlet discovery = new DiscoveryServiceServlet();
        Field zkCacheField = DiscoveryServiceServlet.class.getDeclaredField("metadataStoreCacheLoader");
        zkCacheField.setAccessible(true);
        MetadataStoreCacheLoader metadataCacheLoader = new MetadataStoreCacheLoader(resources, 30_000);
        zkCacheField.set(discovery, metadataCacheLoader);

        // 3. verify nextBroker functionality : round-robin in broker list
        for (String broker : brokers) {
            assertEquals(broker, discovery.nextBroker().getWebServiceUrl());
        }
    }

    @Test
    public void testRiderectUrlWithServerStarted() throws Exception {

        // 1. start server
        ServiceConfig config = new ServiceConfig();
        config.setWebServicePort(Optional.of(0));
        ServerManager server = new ServerManager(config);
        Map<String, String> params = new TreeMap<>();
        String zkServerUrl = "mockZkServer";
        metadataStoreInstanceCache.put(zkServerUrl, zkStore);
        params.put("zookeeperServers", zkServerUrl);
        server.addServlet("/", DiscoveryServiceServletTest.class, params);
        server.start();

        // 2. create znode for each broker
        List<String> brokers = Lists.newArrayList("broker-1", "broker-2", "broker-3");
        brokers.stream().forEach(b -> {
            try {
                final String broker = b + ":15000";
                LoadReport report = new LoadReport("http://" + broker, null, null, null);
                String reportData = ObjectMapperFactory.getThreadLocal().writeValueAsString(report);
                zkStore.put(LOADBALANCE_BROKERS_ROOT + "/" + broker,
                        reportData.getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();
            }  catch (Exception e) {
                if (e instanceof ExecutionException && (e.getCause()) instanceof AlreadyExistsException) {
                    // Ok
                } else {
                    log.warn("Failed to write to metadata-store", e);
                    fail("failed while creating broker znodes");
                }
            }
        });

        String serviceUrl = server.getServiceUri().toString();
        String requestUrl = serviceUrl + "admin/namespaces/p1/c1/n1";

        /**
         * 3. verify : every time when vip receives a request: it redirects to above brokers sequentially and client
         * must get unknown host exception with above brokers in a sequential manner.
         **/

        assertEquals(brokers, validateRequest(brokers, HttpMethod.PUT, requestUrl, BundlesData.builder().numBundles(1).build()),
                "redirection failed");
        assertEquals(brokers, validateRequest(brokers, HttpMethod.GET, requestUrl, null), "redirection failed");
        assertEquals(brokers, validateRequest(brokers, HttpMethod.POST, requestUrl, BundlesData.builder().numBundles(1).build()),
                "redirection failed");

        server.stop();

    }


    @Test
    public void testTlsEnable() throws Exception {

        // 1. start server with tls enable
        ServiceConfig config = new ServiceConfig();
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        ServerManager server = new ServerManager(config);
        Map<String, String> params = new TreeMap<>();
        String zkServerUrl = "mockZkServer";
        metadataStoreInstanceCache.put(zkServerUrl, zkStore);
        params.put("zookeeperServers", zkServerUrl);
        server.addServlet("/", DiscoveryServiceServletTest.class, params);

        // 2. get ZookeeperCacheLoader to add more brokers
        final String redirect_broker_host = "broker-1";
        List<String> brokers = Lists.newArrayList(redirect_broker_host);
        brokers.stream().forEach(b -> {
            try {
                final String brokerUrl = b + ":" + server.getListenPortHTTP();
                final String brokerUrlTls = b + ":" + server.getListenPortHTTPS();

                LoadReport report = new LoadReport("http://" + brokerUrl, "https://" + brokerUrlTls, null, null);
                String reportData = ObjectMapperFactory.getThreadLocal().writeValueAsString(report);
                zkStore.put(LOADBALANCE_BROKERS_ROOT + "/" + brokerUrl,
                        reportData.getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();
            }  catch (Exception e) {
                if (e instanceof ExecutionException && (e.getCause()) instanceof AlreadyExistsException) {
                    // Ok
                } else {
                    log.warn("Failed to write to metadata-store", e);
                    fail("failed while creating broker znodes");
                }
            }
        });

        // 3. https request with tls enable at server side
        String serviceUrl = String.format("https://localhost:%s/", server.getListenPortHTTPS());
        String requestUrl = serviceUrl + "admin/namespaces/p1/c1/n1";

        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(keyManagers, trustManagers, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sslCtx.getSocketFactory());
        try {
            InputStream response = new URL(requestUrl).openStream();
            fail("it should give unknown host exception as: discovery service redirects request to: "
                    + redirect_broker_host);
        } catch (Exception e) {
        }

        server.stop();
    }

    @Test
    public void testException() {
        RestException exception1 = new RestException(BAD_GATEWAY, "test-msg");
        assertTrue(exception1.getMessage().contains("test-msg"));
        RestException exception2 = new RestException(BAD_GATEWAY.getStatusCode(), "test-msg");
        assertTrue(exception2.getMessage().contains("test-msg"));
        RestException exception3 = new RestException(exception2);
        assertTrue(exception3.getMessage().contains(BAD_GATEWAY.toString()));
    }

    public List<String> validateRequest(List<String> brokers, String method, String url, BundlesData bundle) {

        List<String> redirectBrokers = brokers.stream().map(broker -> {

            String redirectedBroker = null;
            try {
                WebTarget webTarget = client.target(url);
                Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
                if (HttpMethod.PUT.equals(method)) {
                    invocationBuilder.put(Entity.entity(bundle, MediaType.APPLICATION_JSON));
                    fail();
                } else if (HttpMethod.GET.equals(method)) {
                    invocationBuilder.get();
                    fail();
                } else if (HttpMethod.POST.equals(method)) {
                    invocationBuilder.post(Entity.entity(bundle, MediaType.APPLICATION_JSON));
                    fail();
                } else {
                    fail("Unsupported http method");
                }
            } catch (Exception e) {

                if (e.getCause() instanceof UnknownHostException) {
                    redirectedBroker = e.getCause().getMessage().split(":")[0];
                } else {
                    // fail
                    fail("Expected to receive UnknownHostException, but received : " + e);
                }
            }
            return redirectedBroker;
        }).collect(Collectors.toList());

        return redirectBrokers;
    }

    
    public static class DiscoveryServiceServletTest extends DiscoveryServiceServlet {
        @Override
        public MetadataStoreExtended createLocalMetadataStore(String zookeeperServers, int operationimeoutMs) throws MetadataStoreException {
            return metadataStoreInstanceCache.get(zookeeperServers);
        }
    }
}
