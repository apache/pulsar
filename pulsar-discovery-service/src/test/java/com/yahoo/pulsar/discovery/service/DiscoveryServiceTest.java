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

import static com.yahoo.pulsar.discovery.service.DiscoveryService.LOADBALANCE_BROKERS_ROOT;
import static javax.ws.rs.core.Response.Status.BAD_GATEWAY;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.apache.bookkeeper.test.PortManager.nextFreePort;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.List;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.common.policies.data.BundlesData;
import com.yahoo.pulsar.common.util.SecurityUtility;
import com.yahoo.pulsar.discovery.service.server.DiscoveryServiceStarter;
import com.yahoo.pulsar.discovery.service.server.ServerManager;
import com.yahoo.pulsar.discovery.service.server.ServiceConfig;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * 1. starts discovery service a. loads broker list from zk 2. http-client calls multiple http request: GET, PUT and
 * POST. 3. discovery service redirects to appropriate brokers in round-robin 4. client receives unknown host exception
 * with redirected broker
 * 
 */
public class DiscoveryServiceTest {

    private Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));
    private static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";

    @Test
    public void testNextBroker() throws Exception {

        List<String> brokers = Lists.newArrayList("broker-1:15000", "broker-2:15000", "broker-3:15000");
        System.setProperty("zookeeperServers", "dummy-value");

        DiscoveryService discovery = new DiscoveryService();
        Field zkCacheField = DiscoveryService.class.getDeclaredField("zkCache");
        zkCacheField.setAccessible(true);
        ZooKeeper zk = ((ZookeeperCacheLoader) zkCacheField.get(discovery)).getLocalZkCache().getZooKeeper();

        // 1. create znode for each broker
        brokers.stream().forEach(b -> {
            try {
                zk.create(LOADBALANCE_BROKERS_ROOT + "/" + b, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ne) {
                // Ok
            } catch (KeeperException | InterruptedException e) {
                fail("failed while creating broker znodes");
            }
        });

        Thread.sleep(200);

        // 2. verify nextBroker functionality : round-robin in broker list
        for (String broker : brokers) {
            assertEquals(broker, discovery.nextBroker());
        }

        zk.close();
    }

    @Test
    public void testRiderectUrlWithServerStarted() throws Exception {

        // 1. start server
        List<String> resources = Lists.newArrayList(DiscoveryService.class.getPackage().getName());
        System.setProperty("zookeeperServers", "dummy-value");
        int port = nextFreePort();
        ServiceConfig config = new ServiceConfig();
        config.setWebServicePort(port);
        ServerManager server = new ServerManager(config);
        server.start(resources);

        // 2. get ZookeeperCacheLoader to add more brokers
        DiscoveryService discovery = new DiscoveryService();
        Field zkCacheField = DiscoveryService.class.getDeclaredField("zkCache");
        zkCacheField.setAccessible(true);
        ZooKeeper zk = ((ZookeeperCacheLoader) zkCacheField.get(discovery)).getLocalZkCache().getZooKeeper();

        List<String> brokers = Lists.newArrayList("broker-1", "broker-2", "broker-3");
        // 3. create znode for each broker
        brokers.stream().forEach(b -> {
            try {
                zk.create(LOADBALANCE_BROKERS_ROOT + "/" + b + ":15000", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ne) {
                // Ok
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                fail("failed while creating broker znodes");
            }
        });

        String serviceUrl = server.getServiceUri().toString();
        String requestUrl = serviceUrl + "admin/namespaces/p1/c1/n1";

        /**
         * verify : every time when vip receives a request: it redirects to above brokers sequentially and client must
         * get unknown host exception with above brokers in a sequential manner.
         **/

        assertEquals(brokers, validateRequest(brokers, HttpMethod.PUT, requestUrl, new BundlesData(1)),
                "redirection failed");
        assertEquals(brokers, validateRequest(brokers, HttpMethod.GET, requestUrl, null), "redirection failed");
        assertEquals(brokers, validateRequest(brokers, HttpMethod.POST, requestUrl, new BundlesData(1)),
                "redirection failed");

        server.stop();

        zk.close();

    }

    @Test
    public void testDiscoveryServiceStarter() throws Exception {

        int port = nextFreePort();
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=z1.yahoo.com,z2.yahoo.com,z3.yahoo.com");
        printWriter.println("webServicePort=" + port);
        printWriter.close();
        testConfigFile.deleteOnExit();
        DiscoveryServiceStarter.main(new String[] { testConfigFile.getAbsolutePath() });
        String host = InetAddress.getLocalHost().getHostAddress();
        String requestUrl = String.format("http://%s:%d/%s", host, port, "admin/namespaces/p1/c1/n1");
        WebTarget webTarget = client.target(requestUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        assertEquals(response.getStatus(), 503);
        testConfigFile.delete();
    }

    @Test
    public void testTlsEnable() throws Exception {

        // 1. start server with tls enable
        final boolean allowInsecure = false;
        List<String> resources = Lists.newArrayList(DiscoveryService.class.getPackage().getName());
        System.setProperty("zookeeperServers", "dummy-value");
        int port = nextFreePort();
        int tlsPort = nextFreePort();
        ServiceConfig config = new ServiceConfig();
        config.setWebServicePort(port);
        config.setWebServicePortTls(tlsPort);
        config.setTlsEnabled(true);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        ServerManager server = new ServerManager(config);
        server.start(resources);

        // 2. get ZookeeperCacheLoader to add more brokers
        DiscoveryService discovery = new DiscoveryService();
        Field zkCacheField = DiscoveryService.class.getDeclaredField("zkCache");
        zkCacheField.setAccessible(true);
        ZooKeeper zk = ((ZookeeperCacheLoader) zkCacheField.get(discovery)).getLocalZkCache().getZooKeeper();
        final String redirect_broker_host = "broker-1";
        List<String> brokers = Lists.newArrayList(redirect_broker_host);
        // 3. create znode for each broker
        brokers.stream().forEach(b -> {
            try {
                zk.create(LOADBALANCE_BROKERS_ROOT + "/" + b + ":" + tlsPort, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ne) {
                // Ok
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                fail("failed while creating broker znodes");
            }
        });

        // 4. https request with tls enable at server side
        String serviceUrl = String.format("https://localhost:%s/", tlsPort);
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
            // 5. Verify: server accepts https request and redirected to one of the available broker host defined into
            // zk. and as broker-service is not up: it should give "UnknownHostException with host=broker-url"
            String host = e.getLocalizedMessage();
            assertEquals(e.getClass(), UnknownHostException.class);
            assertTrue(host.startsWith(redirect_broker_host));
        }

        server.stop();
    }

    @Test
    public void testException() {

        RestException exception1 = new RestException(BAD_GATEWAY, "test-msg");
        assertTrue(exception1.getMessage().contains(BAD_GATEWAY.toString()));
        RestException exception2 = new RestException(BAD_GATEWAY.getStatusCode(), "test-msg");
        assertTrue(exception2.getMessage().contains(BAD_GATEWAY.toString()));
        RestException exception3 = new RestException(exception2);
        assertTrue(exception3.getMessage().contains(INTERNAL_SERVER_ERROR.toString()));
        assertTrue(RestException.getExceptionData(exception2).contains(BAD_GATEWAY.toString()));
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
                    fail();
                }
            }
            return redirectedBroker;
        }).collect(Collectors.toList());

        return redirectBrokers;
    }

}
