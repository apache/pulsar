/*
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
package org.apache.pulsar.metrics.prometheus.zookeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.metrics.CounterSet;
import org.apache.zookeeper.metrics.Gauge;
import org.apache.zookeeper.metrics.GaugeSet;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.metrics.Summary;
import org.apache.zookeeper.metrics.SummarySet;
import org.apache.zookeeper.server.util.QuotaMetricsUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests about Prometheus Metrics Provider. Please note that we are not testing
 * Prometheus but only our integration.
 */
public class PrometheusMetricsProviderTest {

    private static final String URL_FORMAT = "http://localhost:%d/metrics";
    private PrometheusMetricsProvider provider;

    @BeforeMethod
    public void setup() throws Exception {
        CollectorRegistry.defaultRegistry.clear();
        provider = new PrometheusMetricsProvider();
        Properties configuration = new Properties();
        configuration.setProperty("numWorkerThreads", "0"); // sync behavior for test
        configuration.setProperty("httpHost", "127.0.0.1"); // local host for test
        configuration.setProperty("httpPort", "0"); // ephemeral port
        configuration.setProperty("exportJvmInfo", "false");
        provider.configure(configuration);
        provider.start();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (provider != null) {
            provider.stop();
        }
        CollectorRegistry.defaultRegistry.clear();
    }

    @Test
    public void testCounters() throws Exception {
        Counter counter = provider.getRootContext().getCounter("cc");
        counter.add(10);
        int[] count = {0};
        provider.dump((k, v) -> {
                    if (k.contains("_created")) {
                        return;
                    }
                    assertEquals("cc_total", k);
                    assertEquals(10, ((Number) v).intValue());
                    count[0]++;
                }
        );
        assertEquals(1, count[0]);
        count[0] = 0;

        // this is not allowed but it must not throw errors
        counter.add(-1);

        provider.dump((k, v) -> {
                    if (k.contains("_created")) {
                        return;
                    }
                    assertEquals("cc_total", k);
                    assertEquals(10, ((Number) v).intValue());
                    count[0]++;
                }
        );
        assertEquals(1, count[0]);

        // we always must get the same object
        assertSame(counter, provider.getRootContext().getCounter("cc"));

        String res = callServlet();
        assertThat(res).contains("# TYPE cc_total counter");
        assertThat(res).contains("cc_total 10.0");
    }

    @Test
    public void testCounterSet_single() throws Exception {
        // create and register a CounterSet
        final String name = QuotaMetricsUtils.QUOTA_EXCEEDED_ERROR_PER_NAMESPACE;
        final CounterSet counterSet = provider.getRootContext().getCounterSet(name);
        final String[] keys = {"ns1", "ns2"};
        final int count = 3;

        // update the CounterSet multiple times
        for (int i = 0; i < count; i++) {
            Arrays.asList(keys).forEach(key -> counterSet.inc(key));
            Arrays.asList(keys).forEach(key -> counterSet.add(key, 2));
        }

        // validate with dump call
        final Map<String, Number> expectedMetricsMap = new HashMap<>();
        for (final String key : keys) {
            expectedMetricsMap.put(String.format("%s_total{key=\"%s\"}", name, key), count * 3.0);
        }
        validateWithDump(expectedMetricsMap);

        // validate with servlet call
        final List<String> expectedNames = Collections.singletonList(String.format("# TYPE %s count", name));
        final List<String> expectedMetrics = new ArrayList<>();
        for (final String key : keys) {
            expectedMetrics.add(String.format("%s{key=\"%s\",} %s", name, key, count * 3.0));
        }
        validateWithServletCall(expectedNames, expectedMetrics);

        // validate registering with same name, no overwriting
        assertSame(counterSet, provider.getRootContext().getCounterSet(name));
    }

    @Test
    public void testCounterSet_multiple() throws Exception {
        final String name = QuotaMetricsUtils.QUOTA_EXCEEDED_ERROR_PER_NAMESPACE;

        final String[] names = new String[]{name + "_1", name + "_2"};
        final String[] keys = new String[]{"ns21", "ns22"};
        final int[] counts = new int[]{3, 5};

        final int length = names.length;
        final CounterSet[] counterSets = new CounterSet[length];

        // create and register the CounterSets
        for (int i = 0; i < length; i++) {
            counterSets[i] = provider.getRootContext().getCounterSet(names[i]);
        }

        // update each CounterSet multiple times
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < counts[i]; j++) {
                counterSets[i].inc(keys[i]);
            }
        }

        // validate with dump call
        final Map<String, Number> expectedMetricsMap = new HashMap<>();
        for (int i = 0; i < length; i++) {
            expectedMetricsMap.put(String.format("%s_total{key=\"%s\"}", names[i], keys[i]), counts[i] * 1.0);
        }
        validateWithDump(expectedMetricsMap);

        // validate with servlet call
        final List<String> expectedNames = new ArrayList<>();
        final List<String> expectedMetrics = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            expectedNames.add(String.format("# TYPE %s count", names[i]));
            expectedMetrics.add(String.format("%s{key=\"%s\",} %s", names[i], keys[i], counts[i] * 1.0));
        }
        validateWithServletCall(expectedNames, expectedMetrics);
    }

    @Test
    public void testCounterSet_registerWithNullName() {
        assertThrows(NullPointerException.class,
                () -> provider.getRootContext().getCounterSet(null));
    }

    @Test
    public void testCounterSet_negativeValue() {
        // create and register a CounterSet
        final String name = QuotaMetricsUtils.QUOTA_EXCEEDED_ERROR_PER_NAMESPACE;
        final CounterSet counterSet = provider.getRootContext().getCounterSet(name);

        // add negative value and make sure no exception is thrown
        counterSet.add("ns1", -1);
    }

    @Test
    public void testCounterSet_nullKey() {
        // create and register a CounterSet
        final String name = QuotaMetricsUtils.QUOTA_EXCEEDED_ERROR_PER_NAMESPACE;
        final CounterSet counterSet = provider.getRootContext().getCounterSet(name);

        // increment the count with null key and make sure no exception is thrown
        counterSet.inc(null);
        counterSet.add(null, 2);
    }

    @Test
    public void testGauge() throws Exception {
        int[] values = {78, -89};
        int[] callCounts = {0, 0};
        Gauge gauge0 = () -> {
            callCounts[0]++;
            return values[0];
        };
        Gauge gauge1 = () -> {
            callCounts[1]++;
            return values[1];
        };
        provider.getRootContext().registerGauge("gg", gauge0);

        int[] count = {0};
        provider.dump((k, v) -> {
                    assertEquals("gg", k);
                    assertEquals(values[0], ((Number) v).intValue());
                    count[0]++;
                }
        );
        assertEquals(1, callCounts[0]);
        assertEquals(0, callCounts[1]);
        assertEquals(1, count[0]);
        count[0] = 0;
        String res2 = callServlet();
        assertThat(res2).contains("# TYPE gg gauge");
        assertThat(res2.contains("gg 78.0"));

        provider.getRootContext().unregisterGauge("gg");
        provider.dump((k, v) -> {
                    count[0]++;
                }
        );
        assertEquals(2, callCounts[0]);
        assertEquals(0, callCounts[1]);
        assertEquals(0, count[0]);
        String res3 = callServlet();
        assertTrue(res3.isEmpty());

        provider.getRootContext().registerGauge("gg", gauge1);

        provider.dump((k, v) -> {
                    assertEquals("gg", k);
                    assertEquals(values[1], ((Number) v).intValue());
                    count[0]++;
                }
        );
        assertEquals(2, callCounts[0]);
        assertEquals(1, callCounts[1]);
        assertEquals(1, count[0]);
        count[0] = 0;

        String res4 = callServlet();
        assertThat(res4.contains("# TYPE gg gauge"));
        assertThat(res4.contains("gg -89.0"));
        assertEquals(2, callCounts[0]);
        // the servlet must sample the value again (from gauge1)
        assertEquals(2, callCounts[1]);

        // override gauge, without unregister
        provider.getRootContext().registerGauge("gg", gauge0);

        provider.dump((k, v) -> {
                    count[0]++;
                }
        );
        assertEquals(1, count[0]);
        assertEquals(3, callCounts[0]);
        assertEquals(2, callCounts[1]);
    }

    @Test
    public void testBasicSummary() throws Exception {
        Summary summary = provider.getRootContext()
                .getSummary("cc", MetricsContext.DetailLevel.BASIC);
        summary.add(10);
        summary.add(10);
        int[] count = {0};
        provider.dump((k, v) -> {
                    count[0]++;
                    int value = ((Number) v).intValue();

                    switch (k) {
                        case "cc{quantile=\"0.5\"}":
                            assertEquals(10, value);
                            break;
                        case "cc_count":
                            assertEquals(2, value);
                            break;
                        case "cc_sum":
                            assertEquals(20, value);
                            break;
                        case "cc_created":
                            break;
                        default:
                            fail("unexpected key " + k);
                            break;
                    }
                }
        );
        assertEquals(4, count[0]);
        count[0] = 0;

        // we always must get the same object
        assertSame(summary, provider.getRootContext()
                .getSummary("cc", MetricsContext.DetailLevel.BASIC));

        try {
            provider.getRootContext()
                    .getSummary("cc", MetricsContext.DetailLevel.ADVANCED);
            fail("Can't get the same summary with a different DetailLevel");
        } catch (IllegalArgumentException err) {
            assertThat(err.getMessage()).contains("Already registered");
        }

        String res = callServlet();
        assertThat(res).contains("# TYPE cc summary");
        assertThat(res).contains("cc_sum 20.0");
        assertThat(res).contains("cc_count 2.0");
        assertThat(res).contains("cc{quantile=\"0.5\",} 10.0");
    }

    @Test
    public void testAdvancedSummary() throws Exception {
        Summary summary = provider.getRootContext()
                .getSummary("cc", MetricsContext.DetailLevel.ADVANCED);
        summary.add(10);
        summary.add(10);
        int[] count = {0};
        provider.dump((k, v) -> {
                    count[0]++;
                    int value = ((Number) v).intValue();

                    switch (k) {
                        case "cc{quantile=\"0.5\"}":
                            assertEquals(10, value);
                            break;
                        case "cc{quantile=\"0.9\"}":
                            assertEquals(10, value);
                            break;
                        case "cc{quantile=\"0.99\"}":
                            assertEquals(10, value);
                            break;
                        case "cc_count":
                            assertEquals(2, value);
                            break;
                        case "cc_sum":
                            assertEquals(20, value);
                            break;
                        case "cc_created":
                            break;
                        default:
                            fail("unexpected key " + k);
                            break;
                    }
                }
        );
        assertEquals(6, count[0]);
        count[0] = 0;

        // we always must get the same object
        assertSame(summary, provider.getRootContext()
                .getSummary("cc", MetricsContext.DetailLevel.ADVANCED));

        try {
            provider.getRootContext()
                    .getSummary("cc", MetricsContext.DetailLevel.BASIC);
            fail("Can't get the same summary with a different DetailLevel");
        } catch (IllegalArgumentException err) {
            assertThat(err.getMessage()).contains("Already registered");
        }

        String res = callServlet();
        assertThat(res).contains("# TYPE cc summary");
        assertThat(res).contains("cc_sum 20.0");
        assertThat(res).contains("cc_count 2.0");
        assertThat(res).contains("cc{quantile=\"0.5\",} 10.0");
        assertThat(res).contains("cc{quantile=\"0.9\",} 10.0");
        assertThat(res).contains("cc{quantile=\"0.99\",} 10.0");
    }

    /**
     * Using TRACE method to visit metrics provider, the response should be 403 forbidden.
     */
    @Test
    public void testTraceCall() throws IOException, IllegalAccessException, NoSuchFieldException {
        Field privateServerField = provider.getClass().getDeclaredField("server");
        privateServerField.setAccessible(true);
        Server server = (Server) privateServerField.get(provider);
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();

        String metricsUrl = String.format(URL_FORMAT, port);
        HttpURLConnection conn = (HttpURLConnection) new URL(metricsUrl).openConnection();
        conn.setRequestMethod("TRACE");
        conn.connect();
        assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
    }

    @Test
    public void testSummary_asyncAndExceedMaxQueueSize() throws Exception {
        final Properties config = new Properties();
        config.setProperty("numWorkerThreads", "1");
        config.setProperty("maxQueueSize", "1");
        config.setProperty("httpPort", "0"); // ephemeral port
        config.setProperty("exportJvmInfo", "false");

        PrometheusMetricsProvider metricsProvider = null;
        try {
            metricsProvider = new PrometheusMetricsProvider();
            metricsProvider.configure(config);
            metricsProvider.start();
            final Summary summary =
                    metricsProvider.getRootContext().getSummary("cc", MetricsContext.DetailLevel.ADVANCED);

            // make sure no error is thrown
            for (int i = 0; i < 10; i++) {
                summary.add(10);
            }
        } finally {
            if (metricsProvider != null) {
                metricsProvider.stop();
            }
        }
    }

    @Test
    public void testSummarySet() throws Exception {
        final String name = "ss";
        final String[] keys = {"ns1", "ns2"};
        final double count = 3.0;

        // create and register a SummarySet
        final SummarySet summarySet = provider.getRootContext()
                .getSummarySet(name, MetricsContext.DetailLevel.BASIC);

        // update the SummarySet multiple times
        for (int i = 0; i < count; i++) {
            Arrays.asList(keys).forEach(key -> summarySet.add(key, 1));
        }

        // validate with dump call
        final Map<String, Number> expectedMetricsMap = new HashMap<>();
        for (final String key : keys) {
            expectedMetricsMap.put(String.format("%s{key=\"%s\",quantile=\"0.5\"}", name, key), 1.0);
            expectedMetricsMap.put(String.format("%s_count{key=\"%s\"}", name, key), count);
            expectedMetricsMap.put(String.format("%s_sum{key=\"%s\"}", name, key), count);
        }
        validateWithDump(expectedMetricsMap);

        // validate with servlet call
        final List<String> expectedNames = Collections.singletonList(String.format("# TYPE %s summary", name));
        final List<String> expectedMetrics = new ArrayList<>();
        for (final String key : keys) {
            expectedMetrics.add(String.format("%s{key=\"%s\",quantile=\"0.5\",} %s", name, key, 1.0));
            expectedMetrics.add(String.format("%s_count{key=\"%s\",} %s", name, key, count));
            expectedMetrics.add(String.format("%s_sum{key=\"%s\",} %s", name, key, count));
        }
        validateWithServletCall(expectedNames, expectedMetrics);

        // validate registering with same name, no overwriting
        assertSame(summarySet, provider.getRootContext()
                .getSummarySet(name, MetricsContext.DetailLevel.BASIC));

        // validate registering with different DetailLevel, not allowed
        try {
            provider.getRootContext()
                    .getSummarySet(name, MetricsContext.DetailLevel.ADVANCED);
            fail("Can't get the same summarySet with a different DetailLevel");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Already registered");
        }
    }

    private String callServlet() throws ServletException, IOException {
        // we are not performing an HTTP request
        // but we are calling directly the servlet
        StringWriter writer = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(writer));
        HttpServletRequest req = mock(HttpServletRequest.class);
        provider.getServlet().doGet(req, response);
        String res = writer.toString();
        return res;
    }

    @Test
    public void testGaugeSet_singleGaugeSet() throws Exception {
        final String name = QuotaMetricsUtils.QUOTA_BYTES_LIMIT_PER_NAMESPACE;
        final Number[] values = {10.0, 100.0};
        final String[] keys = {"ns11", "ns12"};
        final Map<String, Number> metricsMap = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            metricsMap.put(keys[i], values[i]);
        }
        final AtomicInteger callCount = new AtomicInteger(0);

        // create and register GaugeSet
        createAndRegisterGaugeSet(name, metricsMap, callCount);

        // validate with dump call
        final Map<String, Number> expectedMetricsMap = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            expectedMetricsMap.put(String.format("%s{key=\"%s\"}", name, keys[i]), values[i]);
        }
        validateWithDump(expectedMetricsMap);
        assertEquals(1, callCount.get());

        // validate with servlet call
        final List<String> expectedNames = Collections.singletonList(String.format("# TYPE %s gauge", name));
        final List<String> expectedMetrics = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            expectedMetrics.add(String.format("%s{key=\"%s\",} %s", name, keys[i], values[i]));
        }
        validateWithServletCall(expectedNames, expectedMetrics);
        assertEquals(2, callCount.get());

        // unregister the GaugeSet
        callCount.set(0);
        provider.getRootContext().unregisterGaugeSet(name);

        // validate with dump call
        validateWithDump(Collections.emptyMap());
        assertEquals(0, callCount.get());

        // validate with servlet call
        validateWithServletCall(new ArrayList<>(), new ArrayList<>());
        assertEquals(0, callCount.get());
    }

    @Test
    public void testGaugeSet_multipleGaugeSets() throws Exception {
        final String[] names = new String[]{
                QuotaMetricsUtils.QUOTA_COUNT_LIMIT_PER_NAMESPACE,
                QuotaMetricsUtils.QUOTA_COUNT_USAGE_PER_NAMESPACE
        };

        final Number[] values = new Number[]{20.0, 200.0};
        final String[] keys = new String[]{"ns21", "ns22"};
        final int count = names.length;
        final AtomicInteger[] callCounts = new AtomicInteger[count];

        // create and register the GaugeSets
        for (int i = 0; i < count; i++) {
            final Map<String, Number> metricsMap = new HashMap<>();
            metricsMap.put(keys[i], values[i]);
            callCounts[i] = new AtomicInteger(0);
            createAndRegisterGaugeSet(names[i], metricsMap, callCounts[i]);
        }

        // validate with dump call
        final Map<String, Number> expectedMetricsMap = new HashMap<>();
        for (int i = 0; i < count; i++) {
            expectedMetricsMap.put(String.format("%s{key=\"%s\"}", names[i], keys[i]), values[i]);
        }
        validateWithDump(expectedMetricsMap);
        for (int i = 0; i < count; i++) {
            assertEquals(1, callCounts[i].get());
        }

        // validate with servlet call
        final List<String> expectedNames = new ArrayList<>();
        final List<String> expectedMetrics = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            expectedNames.add(String.format("# TYPE %s gauge", names[i]));
            expectedMetrics.add(String.format("%s{key=\"%s\",} %s", names[i], keys[i], values[i]));
        }
        validateWithServletCall(expectedNames, expectedMetrics);
        for (int i = 0; i < count; i++) {
            assertEquals(2, callCounts[i].get());
        }

        // unregister the GaugeSets
        for (int i = 0; i < count; i++) {
            callCounts[i].set(0);
            provider.getRootContext().unregisterGaugeSet(names[i]);
        }

        // validate with dump call
        validateWithDump(Collections.emptyMap());
        for (int i = 0; i < count; i++) {
            assertEquals(0, callCounts[i].get());
        }

        // validate with servlet call
        validateWithServletCall(new ArrayList<>(), new ArrayList<>());
        for (int i = 0; i < count; i++) {
            assertEquals(0, callCounts[i].get());
        }
    }

    @Test
    public void testGaugeSet_overwriteRegister() {
        final String[] names = new String[]{
                QuotaMetricsUtils.QUOTA_COUNT_LIMIT_PER_NAMESPACE,
                QuotaMetricsUtils.QUOTA_COUNT_USAGE_PER_NAMESPACE
        };

        final int count = names.length;
        final Number[] values = new Number[]{30.0, 300.0};
        final String[] keys = new String[]{"ns31", "ns32"};
        final AtomicInteger[] callCounts = new AtomicInteger[count];

        // create and register the GaugeSets
        for (int i = 0; i < count; i++) {
            final Map<String, Number> metricsMap = new HashMap<>();
            metricsMap.put(keys[i], values[i]);
            callCounts[i] = new AtomicInteger(0);
            // use the same name so the first GaugeSet got overwrite
            createAndRegisterGaugeSet(names[0], metricsMap, callCounts[i]);
        }

        // validate with dump call to make sure the second GaugeSet overwrites the first
        final Map<String, Number> expectedMetricsMap = new HashMap<>();
        expectedMetricsMap.put(String.format("%s{key=\"%s\"}", names[0], keys[1]), values[1]);
        validateWithDump(expectedMetricsMap);
        assertEquals(0, callCounts[0].get());
        assertEquals(1, callCounts[1].get());
    }

    @Test
    public void testGaugeSet_nullKey() {
        final String name = QuotaMetricsUtils.QUOTA_COUNT_LIMIT_PER_NAMESPACE;
        final Map<String, Number> metricsMap = new HashMap<>();
        metricsMap.put(null, 10.0);

        final AtomicInteger callCount = new AtomicInteger(0);

        // create and register GaugeSet
        createAndRegisterGaugeSet(name, metricsMap, callCount);

        // validate with dump call
        assertThrows(IllegalArgumentException.class, () -> provider.dump(new HashMap<>()::put));

        // validate with servlet call
        assertThrows(IllegalArgumentException.class, this::callServlet);
    }

    @Test
    public void testGaugeSet_registerWithNullGaugeSet() {
        assertThrows(NullPointerException.class,
                () -> provider.getRootContext().registerGaugeSet("name", null));

        assertThrows(NullPointerException.class,
                () -> provider.getRootContext().registerGaugeSet(null, HashMap::new));
    }

    @Test
    public void testGaugeSet_unregisterNull() {
        assertThrows(NullPointerException.class,
                () -> provider.getRootContext().unregisterGaugeSet(null));
    }

    private void createAndRegisterGaugeSet(final String name,
                                           final Map<String, Number> metricsMap,
                                           final AtomicInteger callCount) {
        final GaugeSet gaugeSet = () -> {
            callCount.addAndGet(1);
            return metricsMap;
        };
        provider.getRootContext().registerGaugeSet(name, gaugeSet);
    }

    private void validateWithDump(final Map<String, Number> expectedMetrics) {
        final Map<String, Object> returnedMetrics = new HashMap<>();
        provider.dump((key, value) -> {
            if (!key.contains("_created{")) {
                returnedMetrics.put(key, value);
            }
        });
        assertThat(returnedMetrics).isEqualTo(expectedMetrics);
    }

    private void validateWithServletCall(final List<String> expectedNames,
                                         final List<String> expectedMetrics) throws Exception {
        final String response = callServlet();
        if (expectedNames.isEmpty() && expectedMetrics.isEmpty()) {
            assertTrue(response.isEmpty());
        } else {
            expectedNames.forEach(name -> assertThat(response.contains(name)));
            expectedMetrics.forEach(metric -> assertThat(response.contains(metric)));
        }
    }
}
