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
package org.apache.pulsar.tests.integration.plugins;

import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestAdditionalServlets extends PulsarTestSuite {

    private static final String NAME = "random";
    private static final String PREFIX = "PULSAR_PREFIX_";
    public static final int SEQUENCE_LENGTH = 7;

    @Override
    public void setupCluster() throws Exception {
        brokerEnvs.put(PREFIX + "additionalServlets", NAME);
        brokerEnvs.put(PREFIX + "additionalServletDirectory", "/pulsar/examples");
        brokerEnvs.put(PREFIX + "randomServletSequenceLength", "" + SEQUENCE_LENGTH);
        brokerEnvs.put(PREFIX + "narExtractionDirectory", "/tmp");

        proxyEnvs.put(PREFIX + "additionalServlets", NAME);
        proxyEnvs.put(PREFIX + "additionalServletDirectory", "/pulsar/examples");
        proxyEnvs.put(PREFIX + "randomServletSequenceLength", "" + SEQUENCE_LENGTH);
        proxyEnvs.put(PREFIX + "narExtractionDirectory", "/tmp");

        super.setupCluster();
    }

    @Test
    public void testBrokerAdditionalServlet() throws Exception {
        BrokerContainer broker = getPulsarCluster().getAnyBroker();
        String host = broker.getHost();
        Integer httpPort = broker.getMappedPort(BrokerContainer.BROKER_HTTP_PORT);

        testAddress(host, httpPort);
    }


    @Test
    public void testProxyAdditionalServlet() throws Exception {
        ProxyContainer proxy = getPulsarCluster().getProxy();

        String host = proxy.getHost();
        Integer httpPort = proxy.getMappedPort(ProxyContainer.BROKER_HTTP_PORT);

        testAddress(host, httpPort);
    }



    private void testAddress(String host, Integer httpPort) throws IOException, InterruptedException, URISyntaxException {
        ExecutorService executor = null;
        try {
            executor = Executors.newSingleThreadExecutor();
            HttpClient httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS)
                    .executor(executor).build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI("http://" + host + ":" + httpPort + "/" + NAME + "/")).GET().build();
            String response = httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
            Assert.assertEquals(IntStream.range(0, SEQUENCE_LENGTH).boxed().collect(Collectors.toSet()),
                    Arrays.stream(response.split(",")).map(Integer::parseInt).collect(Collectors.toSet()));
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }
}
