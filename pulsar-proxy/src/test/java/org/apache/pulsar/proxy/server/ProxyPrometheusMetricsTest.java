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

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.prometheus.client.Counter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyPrometheusMetricsTest extends MockedPulsarServiceBaseTest {

    public static final String TEST_CLUSTER = "test-cluster";
    private ProxyService proxyService;
    private WebServer proxyWebServer;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(TEST_CLUSTER);

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        proxyService.start();

        proxyService.addPrometheusRawMetricsProvider(stream -> stream.write("test_metrics{label1=\"xyz\"} 10 \n"));

        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        proxyWebServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(proxyWebServer, proxyConfig, proxyService, null);
        proxyWebServer.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        proxyWebServer.stop();
        proxyService.close();
    }

    /**
     * Validates proxy Prometheus endpoint.
     */
    @Test
    public void testMetrics() {
        Counter.build("test_counter", "a test counter").create().register();

        Client httpClient = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
        Response r = httpClient.target(proxyWebServer.getServiceUri()).path("/metrics").request()
                .get();
        Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
        String response = r.readEntity(String.class).trim();

        Multimap<String, Metric> metrics = parseMetrics(response);

        // Check that ProxyService metrics are present
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_proxy_binary_bytes");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), TEST_CLUSTER);

        // Check that any Prometheus metric registered in the default CollectorRegistry is present
        List<Metric> cm2 = (List<Metric>) metrics.get("test_metrics");
        assertEquals(cm2.size(), 1);
        assertEquals(cm2.get(0).tags.get("label1"), "xyz");

        // Check that PrometheusRawMetricsProvider metrics are present
        List<Metric> cm3 = (List<Metric>) metrics.get("test_counter");
        assertEquals(cm3.size(), 1);
        assertEquals(cm3.get(0).tags.get("cluster"), TEST_CLUSTER);
    }

    /**
     * Hacky parsing of Prometheus text format. Should be good enough for unit tests
     */
    public static Multimap<String, Metric> parseMetrics(String metrics) {
        Multimap<String, Metric> parsed = ArrayListMultimap.create();

        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="sample/standalone/ns1",
        // topic="persistent://sample/standalone/ns1/test-2"} 0.0
        Pattern pattern = Pattern.compile("^(\\w+)\\{([^\\}]+)\\}\\s([+-]?[\\d\\w\\.-]+)$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");

        Splitter.on("\n").split(metrics).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }

            Matcher matcher = pattern.matcher(line);
            assertTrue(matcher.matches(), "line " + line + " does not match pattern " + pattern);
            String name = matcher.group(1);

            Metric m = new Metric();
            String numericValue = matcher.group(3);
            if (numericValue.equalsIgnoreCase("-Inf")) {
                m.value = Double.NEGATIVE_INFINITY;
            } else if (numericValue.equalsIgnoreCase("+Inf")) {
                m.value = Double.POSITIVE_INFINITY;
            } else {
                m.value = Double.parseDouble(numericValue);
            }
            String tags = matcher.group(2);
            Matcher tagsMatcher = tagsPattern.matcher(tags);
            while (tagsMatcher.find()) {
                String tag = tagsMatcher.group(1);
                String value = tagsMatcher.group(2);
                m.tags.put(tag, value);
            }

            parsed.put(name, m);
        });

        return parsed;
    }

    static class Metric {
        Map<String, String> tags = new TreeMap<>();
        double value;

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("tags", tags).add("value", value).toString();
        }
    }

}
