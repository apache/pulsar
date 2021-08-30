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
package org.apache.pulsar.broker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlet;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServletWithClassLoader;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServletWithPulsarService;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlets;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.servlet.ServletHolder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class BrokerAdditionalServletTest extends MockedPulsarServiceBaseTest {

    private final String BASE_PATH = "/additional/servlet";
    private final String WITH_PULSAR_SERVICE_BASE_PATH = "/additional/servlet/with/pulsar/service";
    private final String QUERY_PARAM = "param";

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Override
    protected void beforePulsarStartMocks(PulsarService pulsar) throws Exception {
        mockAdditionalServlet(pulsar);
    }

    private void mockAdditionalServlet(PulsarService pulsar) {
        Servlet servlet = new OrdinaryServlet();

        AdditionalServlet brokerAdditionalServlet = Mockito.mock(AdditionalServlet.class);
        Mockito.when(brokerAdditionalServlet.getBasePath()).thenReturn(BASE_PATH);
        Mockito.when(brokerAdditionalServlet.getServletHolder()).thenReturn(new ServletHolder(servlet));

        AdditionalServletWithPulsarService brokerAdditionalServletWithPulsarService =
                new AdditionalServletWithPulsarService() {
                    private PulsarService pulsarService;
                    @Override
                    public void setPulsarService(PulsarService pulsarService) {
                        this.pulsarService = pulsarService;
                    }

                    @Override
                    public void loadConfig(PulsarConfiguration pulsarConfiguration) {
                        // No-op
                    }

                    @Override
                    public String getBasePath() {
                        return WITH_PULSAR_SERVICE_BASE_PATH;
                    }

                    @Override
                    public ServletHolder getServletHolder() {
                        return new ServletHolder(new WithPulsarServiceServlet(pulsarService));
                    }

                    @Override
                    public void close() {
                        // No-op
                    }
                };


        AdditionalServlets brokerAdditionalServlets = Mockito.mock(AdditionalServlets.class);
        Map<String, AdditionalServletWithClassLoader> map = new HashMap<>();
        map.put("broker-additional-servlet", new AdditionalServletWithClassLoader(brokerAdditionalServlet, null));
        map.put("broker-additional-servlet-with-pulsar-service", new AdditionalServletWithClassLoader(brokerAdditionalServletWithPulsarService, null));
        Mockito.when(brokerAdditionalServlets.getServlets()).thenReturn(map);

        Mockito.when(pulsar.getBrokerAdditionalServlets()).thenReturn(brokerAdditionalServlets);
    }

    @Test
    public void test() throws IOException {
        int httpPort = pulsar.getWebService().getListenPortHTTP().get();
        log.info("pulsar webService httpPort {}", httpPort);
        String paramValue = "value - " + RandomUtils.nextInt();
        String response = httpGet("http://localhost:" + httpPort + BASE_PATH + "?" + QUERY_PARAM + "=" + paramValue);
        Assert.assertEquals(response, paramValue);

        String WithPulsarServiceParamValue = PulsarService.class.getName();
        String withPulsarServiceResponse = httpGet("http://localhost:" + httpPort + WITH_PULSAR_SERVICE_BASE_PATH);
        Assert.assertEquals(WithPulsarServiceParamValue, withPulsarServiceResponse);
    }


    private class OrdinaryServlet implements Servlet {
        @Override
        public void init(ServletConfig servletConfig) throws ServletException {
            log.info("[init]");
        }

        @Override
        public ServletConfig getServletConfig() {
            log.info("[getServletConfig]");
            return null;
        }

        @Override
        public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException,
                IOException {
            log.info("[service] path: {}", ((Request) servletRequest).getOriginalURI());
            String value = servletRequest.getParameterMap().get(QUERY_PARAM)[0];
            ServletOutputStream servletOutputStream = servletResponse.getOutputStream();
            servletResponse.setContentLength(value.getBytes().length);
            servletOutputStream.write(value.getBytes());
            servletOutputStream.flush();
        }

        @Override
        public String getServletInfo() {
            log.info("[getServletInfo]");
            return null;
        }

        @Override
        public void destroy() {
            log.info("[destroy]");
        }
    }


    private class WithPulsarServiceServlet extends OrdinaryServlet {
        private final PulsarService pulsarService;

        public WithPulsarServiceServlet(PulsarService pulsar) {
            this.pulsarService = pulsar;
        }

        @Override
        public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException,
                IOException {
            log.info("[service] path: {}", ((Request) servletRequest).getOriginalURI());
            String value = pulsarService == null ? "null" : PulsarService.class.getName();
            ServletOutputStream servletOutputStream = servletResponse.getOutputStream();
            servletResponse.setContentLength(value.getBytes().length);
            servletOutputStream.write(value.getBytes());
            servletOutputStream.flush();
        }
    }

    String httpGet(String url) throws IOException {
        OkHttpClient client = new OkHttpClient();
        okhttp3.Request request = new okhttp3.Request.Builder()
                .get()
                .url(url)
                .build();

        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        }
    }

}
