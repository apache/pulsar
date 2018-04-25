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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.AssertTrue;
import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.eclipse.jetty.servlet.ServletHolder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class UnauthedAdminProxyHandlerTest extends MockedPulsarServiceBaseTest {
    private final String DUMMY_VALUE = "DUMMY_VALUE";
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private AdminProxyWrapper adminProxyHandler;
    private WebServer webServer;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        // enable tls and auth&auth at broker

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("localhost");
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);
        conf.setClusterName(configClusterName);

        super.init();

        // start proxy service
        proxyConfig.setServicePort(PortManager.nextFreePort());
        proxyConfig.setWebServicePort(PortManager.nextFreePort());
        proxyConfig.setBrokerServiceURL(brokerUrl.toString());

        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setGlobalZookeeperServers(DUMMY_VALUE);

        webServer = new WebServer(proxyConfig);

        adminProxyHandler = new AdminProxyWrapper(proxyConfig);
        ServletHolder servletHolder = new ServletHolder(adminProxyHandler);
        servletHolder.setInitParameter("preserveHost", "true");
        servletHolder.setInitParameter("proxyTo", brokerUrl.toString());
        webServer.addServlet("/admin/*", servletHolder);
        webServer.addServlet("/lookup/*", servletHolder);

        // start web-service
        webServer.start();

    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        internalCleanup();
        webServer.stop();
    }

    @Test
    public void testUnauthenticatedProxy() throws Exception {
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://127.0.0.1:" + proxyConfig.getWebServicePort())
            .build();
        List<String> activeBrokers = admin.brokers().getActiveBrokers(configClusterName);
        Assert.assertEquals(activeBrokers.size(), 1);
        Assert.assertTrue(adminProxyHandler.rewriteCalled);
    }

    static class AdminProxyWrapper extends AdminProxyHandler {
        boolean rewriteCalled = false;

        AdminProxyWrapper(ProxyConfiguration config) {
            super(config);
        }

        @Override
        protected String rewriteTarget(HttpServletRequest clientRequest) {
            rewriteCalled = true;
            return super.rewriteTarget(clientRequest);
        }

    }

}