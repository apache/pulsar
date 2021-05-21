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
package org.apache.pulsar.proxy.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BooleanSupplier;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ProcessorUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyIsAHttpProxyTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(ProxyIsAHttpProxyTest.class);

    private Server backingServer1;
    private Server backingServer2;
    private PulsarResources resource;
    private Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();
        // Set number of CPU's to two for unit tests for running in resource constrained env.
        ProcessorUtils.setAvailableProcessors(2);

        resource = new PulsarResources(new ZKMetadataStore(mockZooKeeper),
                new ZKMetadataStore(mockZooKeeperGlobal));
        backingServer1 = new Server(0);
        backingServer1.setHandler(newHandler("server1"));
        backingServer1.start();

        backingServer2 = new Server(0);
        backingServer2.setHandler(newHandler("server2"));
        backingServer2.start();
    }

    private static AbstractHandler newHandler(String text) {
        return new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest,
                               HttpServletRequest request,HttpServletResponse response)
                    throws IOException, ServletException {
                response.setContentType("text/plain;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
                response.getWriter().println(String.format("%s,%s", text, request.getRequestURI()));
            }
        };
    }

    private static ServletContextHandler newStreamingHandler(LinkedBlockingQueue<String> dataQueue) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        ServletHolder asyncHolder = new ServletHolder(new HttpServlet() {
                @Override
                protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                        throws ServletException, IOException {
                    final AsyncContext ctx = req.startAsync();
                    resp.setContentType("text/plain;charset=utf-8");
                    resp.setStatus(HttpServletResponse.SC_OK);

                    ctx.start(() -> {
                            log.info("Doing async processing");
                            try {
                                while (true) {
                                    String data = dataQueue.take();
                                    if (data.equals("DONE")) {
                                        ctx.complete();
                                        break;
                                    } else {
                                        ctx.getResponse().getWriter().print(data);
                                        ctx.getResponse().getWriter().flush();
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                log.error("Async handler interrupted");
                                ctx.complete();
                            } catch (Exception e) {
                                log.error("Unexpected error in async handler", e);
                                ctx.complete();
                            }
                        });
                }
            });
        asyncHolder.setAsyncSupported(true);
        context.addServlet(asyncHolder, "/");
        return context;
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();

        backingServer1.stop();
        backingServer2.stop();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRedirectNotSpecified() throws Exception {
        Properties props = new Properties();

        props.setProperty("httpReverseProxy.foobar.path", "/ui");
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPathNotSpecified() throws Exception {
        Properties props = new Properties();

        props.setProperty("httpReverseProxy.foobar.proxyTo", backingServer1.getURI().toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
    }

    @Test
    public void testSingleRedirect() throws Exception {
        Properties props = new Properties();

        props.setProperty("httpReverseProxy.foobar.path", "/ui");
        props.setProperty("httpReverseProxy.foobar.proxyTo", backingServer1.getURI().toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/ui/foobar").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r.readEntity(String.class).trim(), "server1,/foobar");
        } finally {
            webServer.stop();
        }
    }

    @Test
    public void testMultipleRedirect() throws Exception {
        Properties props = new Properties();

        props.setProperty("httpReverseProxy.0.path", "/server1");
        props.setProperty("httpReverseProxy.0.proxyTo", backingServer1.getURI().toString());
        props.setProperty("httpReverseProxy.1.path", "/server2");
        props.setProperty("httpReverseProxy.1.proxyTo", backingServer2.getURI().toString());

        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));
        webServer.start();
        try {
            Response r1 = client.target(webServer.getServiceUri()).path("/server1/foobar").request().get();
            Assert.assertEquals(r1.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r1.readEntity(String.class).trim(), "server1,/foobar");

            Response r2 = client.target(webServer.getServiceUri()).path("/server2/blahblah").request().get();
            Assert.assertEquals(r2.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r2.readEntity(String.class).trim(), "server2,/blahblah");

        } finally {
            webServer.stop();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTryingToUseExistingPath() throws Exception {
        Properties props = new Properties();

        props.setProperty("httpReverseProxy.foobar.path", "/admin");
        props.setProperty("httpReverseProxy.foobar.proxyTo", backingServer1.getURI().toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));

    }

    @Test
    public void testLongPathInProxyTo() throws Exception {
        Properties props = new Properties();
        props.setProperty("httpReverseProxy.foobar.path", "/ui");
        props.setProperty("httpReverseProxy.foobar.proxyTo",
                          backingServer1.getURI().resolve("/foo/bar/blah/yadda/yadda/yadda").toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/ui/foobar").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r.readEntity(String.class).trim(), "server1,/foo/bar/blah/yadda/yadda/yadda/foobar");
        } finally {
            webServer.stop();
        }

    }

    @Test
    public void testProxyToEndsInSlash() throws Exception {
        Properties props = new Properties();
        props.setProperty("httpReverseProxy.foobar.path", "/ui");
        props.setProperty("httpReverseProxy.foobar.proxyTo",
                          backingServer1.getURI().resolve("/foo/").toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/ui/foobar").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r.readEntity(String.class).trim(), "server1,/foo/foobar");
        } finally {
            webServer.stop();
        }

    }

    @Test
    public void testLongPath() throws Exception {
        Properties props = new Properties();
        props.setProperty("httpReverseProxy.foobar.path", "/foo/bar/blah");
        props.setProperty("httpReverseProxy.foobar.proxyTo", backingServer1.getURI().toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/foo/bar/blah/foobar").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r.readEntity(String.class).trim(), "server1,/foobar");
        } finally {
            webServer.stop();
        }
    }

    @Test
    public void testPathEndsInSlash() throws Exception {
        Properties props = new Properties();
        props.setProperty("httpReverseProxy.foobar.path", "/ui/");
        props.setProperty("httpReverseProxy.foobar.proxyTo", backingServer1.getURI().toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/ui/foobar").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r.readEntity(String.class).trim(), "server1,/foobar");
        } finally {
            webServer.stop();
        }

    }

    @Test
    public void testStreaming() throws Exception {
        LinkedBlockingQueue<String> dataQueue = new LinkedBlockingQueue<>();
        Server streamingServer = new Server(0);
        streamingServer.setHandler(newStreamingHandler(dataQueue));
        streamingServer.start();

        Properties props = new Properties();
        props.setProperty("httpOutputBufferSize", "1");
        props.setProperty("httpReverseProxy.foobar.path", "/stream");
        props.setProperty("httpReverseProxy.foobar.proxyTo", streamingServer.getURI().toString());
        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, null,
                new BrokerDiscoveryProvider(proxyConfig, resource));
        webServer.start();

        HttpClient httpClient = new HttpClient();
        httpClient.start();
        try {
            LinkedBlockingQueue<Byte> responses = new LinkedBlockingQueue<>();
            CompletableFuture<Result> promise = new CompletableFuture<>();
            httpClient.newRequest(webServer.getServiceUri()).path("/stream")
                .onResponseContent((response, content) -> {
                        while (content.hasRemaining()) {
                            try {
                                responses.put(content.get());
                            } catch (Exception e) {
                                log.error("Error reading response", e);
                                promise.completeExceptionally(e);
                            }
                        }
                    })
                .send((result) -> {
                        log.info("Response complete");
                        promise.complete(result);
                    });

            dataQueue.put("Some data");
            assertEventuallyTrue(() -> responses.size() == "Some data".length());
            Assert.assertEquals("Some data", drainToString(responses));
            Assert.assertFalse(promise.isDone());

            dataQueue.put("More data");
            assertEventuallyTrue(() -> responses.size() == "More data".length());
            Assert.assertEquals("More data", drainToString(responses));
            Assert.assertFalse(promise.isDone());

            dataQueue.put("DONE");
            assertEventuallyTrue(() -> promise.isDone());
            Assert.assertTrue(promise.get().isSucceeded());
        } finally {
            webServer.stop();
            httpClient.stop();
            streamingServer.stop();
        }
    }

    static String drainToString(Queue<Byte> queue) throws Exception {
        byte[] bytes = new byte[queue.size()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = queue.poll();
        }
        return new String(bytes, UTF_8);
    }

     static void assertEventuallyTrue(BooleanSupplier predicate) throws Exception {
        // wait up to 3 seconds
        for (int i = 0; i < 30 && !predicate.getAsBoolean(); i++) {
            Thread.sleep(100);
        }
        Assert.assertTrue(predicate.getAsBoolean());
    }
}
