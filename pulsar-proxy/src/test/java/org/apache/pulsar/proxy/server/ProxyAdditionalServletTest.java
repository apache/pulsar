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

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServletWithClassLoader;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlets;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlet;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.servlet.ServletHolder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;

@Slf4j
public class ProxyAdditionalServletTest extends MockedPulsarServiceBaseTest {

    private final String BASE_PATH = "/metrics/broker";
    private final String QUERY_PARAM = "param";

    private ProxyService proxyService;
    private WebServer proxyWebServer;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(GLOBAL_DUMMY_VALUE);
        // enable full parsing feature
        proxyConfig.setProxyLogLevel(Optional.of(2));

        // this is for nar package test
//        addServletNar();

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        Optional<Integer> proxyLogLevel = Optional.of(2);
        assertEquals(proxyLogLevel, proxyService.getConfiguration().getProxyLogLevel());
        proxyService.start();

        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        mockAdditionalServlet();

        proxyWebServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(proxyWebServer, proxyConfig, proxyService, null);
        proxyWebServer.start();
    }

    // this is for nar package test
    private void addServletNar() {
        Properties properties = new Properties();
        properties.setProperty("basePath", "/metrics-prometheus/broker");
        properties.setProperty("mappedPath", "/federate");
        properties.setProperty("query", "match[]={job=\"prometheus\"}");
        proxyConfig.setProperties(properties);

        // set protocol related config
        URL testHandlerUrl = this.getClass().getClassLoader().getResource("proxy-additional-servlet-plugin-1.0-SNAPSHOT.nar");
        Path handlerPath;
        try {
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            return;
        }
        String servletDirectory = handlerPath.toFile().getParent();
        proxyConfig.setProxyAdditionalServletDirectory(servletDirectory);

        proxyConfig.setProxyAdditionalServlets(Sets.newHashSet("prometheus-proxy-servlet"));
    }

    private void mockAdditionalServlet() {
        Servlet servlet = new Servlet() {
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
            public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
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
        };

        AdditionalServlet proxyAdditionalServlet = Mockito.mock(AdditionalServlet.class);
        Mockito.when(proxyAdditionalServlet.getBasePath()).thenReturn(BASE_PATH);
        Mockito.when(proxyAdditionalServlet.getServletHolder()).thenReturn(new ServletHolder(servlet));

        AdditionalServlets proxyAdditionalServlets = Mockito.mock(AdditionalServlets.class);
        Map<String, AdditionalServletWithClassLoader> map = new HashMap<>();
        map.put("prometheus-proxy-servlet", new AdditionalServletWithClassLoader(proxyAdditionalServlet, null));
        Mockito.when(proxyAdditionalServlets.getServlets()).thenReturn(map);

        Mockito.when(proxyService.getProxyAdditionalServlets()).thenReturn(proxyAdditionalServlets);
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
    }

    @Test
    public void test() throws IOException {
        int httpPort = proxyWebServer.getListenPortHTTP().get();
        log.info("proxy service httpPort {}", httpPort);
        String paramValue = "value - " + RandomUtils.nextInt();
        String response = httpGet("http://localhost:" + httpPort + BASE_PATH + "?" + QUERY_PARAM + "=" + paramValue);
        Assert.assertEquals(response, paramValue);
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
