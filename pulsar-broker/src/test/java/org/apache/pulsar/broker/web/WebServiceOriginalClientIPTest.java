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
package org.apache.pulsar.broker.web;

import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import nl.altindag.console.ConsoleCaptor;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.assertj.core.api.ThrowingConsumer;
import org.awaitility.Awaitility;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.ProxyProtocolClientConnectionFactory.V2;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class WebServiceOriginalClientIPTest extends MockedPulsarServiceBaseTest {
    HttpClient httpClient;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        httpClient = new HttpClient(new SslContextFactory(true));
        httpClient.start();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (httpClient != null) {
            httpClient.stop();
        }
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setWebServiceTrustXForwardedFor(true);
        conf.setWebServiceHaProxyProtocolEnabled(true);
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
    }

    @DataProvider(name = "tlsEnabled")
    public Object[][] tlsEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "tlsEnabled")
    public void testClientIPIsPickedFromXForwardedForHeaderAndLogged(boolean tlsEnabled) throws Exception {
        String metricsUrl =
                (tlsEnabled ? pulsar.getWebServiceAddressTls() : pulsar.getWebServiceAddress()) + "/metrics/";
        performLoggingTest(consoleCaptor -> {
            // Send a GET request to the metrics URL
            ContentResponse response = httpClient.newRequest(metricsUrl)
                    .header("X-Forwarded-For", "11.22.33.44:12345")
                    .send();

            // Validate the response
            assertTrue(response.getContentAsString().contains("process_cpu_seconds_total"));

            // Validate that the client IP passed in X-Forwarded-For is logged
            assertTrue(consoleCaptor.getStandardOutput().stream()
                    .anyMatch(line -> line.contains("RequestLog") && line.contains("[R:11.22.33.44:12345 via ")));
        });
    }

    @Test(dataProvider = "tlsEnabled")
    public void testClientIPIsPickedFromForwardedHeaderAndLogged(boolean tlsEnabled) throws Exception {
        String metricsUrl =
                (tlsEnabled ? pulsar.getWebServiceAddressTls() : pulsar.getWebServiceAddress()) + "/metrics/";
        performLoggingTest(consoleCaptor -> {
            // Send a GET request to the metrics URL
            ContentResponse response = httpClient.newRequest(metricsUrl)
                    .header("Forwarded", "for=11.22.33.44:12345")
                    .send();

            // Validate the response
            assertTrue(response.getContentAsString().contains("process_cpu_seconds_total"));

            // Validate that the client IP passed in Forwarded is logged
            assertTrue(consoleCaptor.getStandardOutput().stream()
                    .anyMatch(line -> line.contains("RequestLog") && line.contains("[R:11.22.33.44:12345 via ")));
        });
    }

    @Test(dataProvider = "tlsEnabled")
    public void testClientIPIsPickedFromHAProxyProtocolAndLogged(boolean tlsEnabled) throws Exception {
        String metricsUrl = (tlsEnabled ? pulsar.getWebServiceAddressTls() : pulsar.getWebServiceAddress()) + "/metrics/";
        performLoggingTest(consoleCaptor -> {
            // Send a GET request to the metrics URL
            ContentResponse response = httpClient.newRequest(metricsUrl)
                    // Jetty client will add HA Proxy protocol header with the given IP to the request
                    .tag(new V2.Tag(V2.Tag.Command.PROXY, null, V2.Tag.Protocol.STREAM,
                            // source IP and port
                            "99.22.33.44", 1234,
                            // destination IP and port
                            "5.4.3.1", 4321,
                            null))
                    .send();

            // Validate the response
            assertTrue(response.getContentAsString().contains("process_cpu_seconds_total"));

            // Validate that the client IP and destination IP passed in HA Proxy protocol is logged
            assertTrue(consoleCaptor.getStandardOutput().stream()
                    .anyMatch(line -> line.contains("RequestLog") && line.contains("[R:99.22.33.44:1234 via ")
                            && line.contains(" dst 5.4.3.1:4321]")));
        });
    }

    void performLoggingTest(ThrowingConsumer<ConsoleCaptor> testFunction) {
        ConsoleCaptor consoleCaptor = new ConsoleCaptor();
        try {
            Awaitility.await().atMost(Duration.of(2, ChronoUnit.SECONDS)).untilAsserted(() -> {
                consoleCaptor.clearOutput();
                testFunction.accept(consoleCaptor);
            });
        } finally {
            consoleCaptor.close();
            System.out.println("--- Captured console output:");
            consoleCaptor.getStandardOutput().forEach(System.out::println);
            consoleCaptor.getErrorOutput().forEach(System.err::println);
            System.out.println("--- End of captured console output");
        }
    }
}
