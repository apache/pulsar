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

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
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
    private Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        log.info("IKDEBUG setting up backing server1");
        backingServer1 = new Server(0);
        backingServer1.setHandler(newHandler("server1"));
        backingServer1.start();
        log.info("IKDEBUG backing server1 started");
        log.info("IKDEBUG setting up backing server2");
        backingServer2 = new Server(0);
        backingServer2.setHandler(newHandler("server2"));
        backingServer2.start();
        log.info("IKDEBUG backing server2 started");
    }

    private static AbstractHandler newHandler(String text) {
        return new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest,
                               HttpServletRequest request,HttpServletResponse response)
                    throws IOException, ServletException {
                log.info("IKDEBUG handler got request {}", request);
                response.setContentType("text/plain;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
                response.getWriter().println(String.format("%s,%s", text, request.getRequestURI()));
            }
        };
    }

    @Override
    @AfterClass
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

    /*@Test(expectedExceptions = IllegalArgumentException.class)
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
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig,
                                                 new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory));
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/ui/foobar").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r.readEntity(String.class).trim(), "server1,/foobar");
        } finally {
            webServer.stop();
        }
        }*/

    @Test
    public void testMultipleRedirect() throws Exception {
        Properties props = new Properties();

        props.setProperty("httpReverseProxy.0.path", "/server1");
        props.setProperty("httpReverseProxy.0.proxyTo", backingServer1.getURI().toString());
        props.setProperty("httpReverseProxy.1.path", "/server2");
        props.setProperty("httpReverseProxy.1.proxyTo", backingServer2.getURI().toString());

        client.target(backingServer1.getURI()).path("/server1").request().get();
        client.target(backingServer2.getURI()).path("/server2").request().get();

        props.setProperty("servicePort", "0");
        props.setProperty("webServicePort", "0");

        log.info("IKDEBUG properties {}", props);
        ProxyConfiguration proxyConfig = PulsarConfigurationLoader.create(props, ProxyConfiguration.class);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        log.info("IKDEBUG starting webserver");
        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig,
                                                 new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory));
        webServer.start();
        log.info("IKDEBUG webserver started");
        try {
            log.info("IKDEBUG request 1");
            Response r1 = client.target(webServer.getServiceUri()).path("/server1/foobar").request().get();
            Assert.assertEquals(r1.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r1.readEntity(String.class).trim(), "server1,/foobar");

            log.info("IKDEBUG request 2");
            Response r2 = client.target(webServer.getServiceUri()).path("/server2/blahblah").request().get();
            Assert.assertEquals(r2.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r2.readEntity(String.class).trim(), "server2,/blahblah");



        } finally {
            webServer.stop();
        }
    }
    /*
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
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig,
                                                 new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory));

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
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig,
                                                 new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory));
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
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig,
                                                 new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory));
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
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig,
                                                 new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory));
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
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig,
                                                 new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory));
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/ui/foobar").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(r.readEntity(String.class).trim(), "server1,/foobar");
        } finally {
            webServer.stop();
        }

    }
    */
}
