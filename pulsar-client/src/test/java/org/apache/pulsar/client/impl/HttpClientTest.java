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
package org.apache.pulsar.client.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.resolver.NameResolver;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.util.netty.DnsResolverUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that {@link HttpClient} carries the {@code Authorization} header across cross-origin HTTP redirects.
 *
 * <p>Pulsar's HTTP lookup endpoint returns {@code 307 Temporary Redirect} to whichever broker owns the bundle for a
 * topic. The redirect target is that broker's {@code httpUrl}/{@code httpUrlTls}, i.e. typically a different host or
 * port from the original request. Auth plugins ({@code AuthenticationToken}, {@code AuthenticationBasic},
 * {@code AuthenticationOAuth2}, {@code AuthenticationAthenz}) inject the {@code Authorization} header — that header
 * must reach the redirect target for lookup to succeed.
 *
 * <p>async-http-client 2.14.5 strips {@code Authorization} on cross-origin redirects when its built-in follow-redirect
 * is enabled (CVE-2026-40490 fix). This test drives two WireMock servers on different ports to exercise that path.
 */
public class HttpClientTest {

    private static final String LOOKUP_PATH = "/lookup/v2/topic/persistent/public/default/test-topic";
    private static final String EXPECTED_BODY = "{\"brokerUrl\":\"pulsar://broker-b:6650\","
            + "\"brokerUrlTls\":\"pulsar+ssl://broker-b:6651\","
            + "\"httpUrl\":\"http://broker-b:8080\","
            + "\"httpUrlTls\":\"https://broker-b:8443\","
            + "\"nativeUrl\":\"pulsar://broker-b:6650\"}";

    private WireMockServer serverA;
    private WireMockServer serverB;
    private EventLoopGroup eventLoopGroup;
    private Timer timer;
    private NameResolver<InetAddress> nameResolver;

    @BeforeClass(alwaysRun = true)
    void beforeClass() {
        eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("HttpClientTest"));
        timer = new HashedWheelTimer(new DefaultThreadFactory("HttpClientTest-timer"));
        // Default JDK-backed resolver is sufficient; we only hit 127.0.0.1.
        nameResolver = DnsResolverUtil.adaptToNameResolver(null);
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
        if (timer != null) {
            timer.stop();
        }
    }

    @BeforeMethod(alwaysRun = true)
    void beforeMethod() {
        serverA = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        serverB = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        serverA.start();
        serverB.start();
    }

    @AfterMethod(alwaysRun = true)
    void afterMethod() {
        if (serverA != null) {
            serverA.stop();
        }
        if (serverB != null) {
            serverB.stop();
        }
    }

    @Test
    public void testCrossOriginRedirectCarriesAuthorizationHeader() throws Exception {
        // serverA (origin host:port) returns 307 Temporary Redirect to serverB (different port -> cross-origin).
        serverA.stubFor(get(urlPathMatching(LOOKUP_PATH))
                .willReturn(aResponse()
                        .withStatus(307)
                        .withHeader("Location",
                                "http://127.0.0.1:" + serverB.port() + LOOKUP_PATH)));

        // serverB only responds 200 when the Authorization header is present with the expected token.
        // Priorities: lower = higher priority; the specific-Authorization stub must be checked first.
        serverB.stubFor(get(urlPathMatching(LOOKUP_PATH))
                .atPriority(2)
                .willReturn(aResponse().withStatus(401).withBody("missing auth")));
        serverB.stubFor(get(urlPathMatching(LOOKUP_PATH))
                .atPriority(1)
                .withHeader("Authorization", equalTo("Bearer test-token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(EXPECTED_BODY)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://127.0.0.1:" + serverA.port());
        conf.setAuthentication(AuthenticationFactory.token("test-token"));

        try (HttpClient httpClient = new HttpClient(conf, eventLoopGroup, timer, nameResolver)) {
            LookupData result = httpClient.get(LOOKUP_PATH, LookupData.class)
                    .get(30, TimeUnit.SECONDS);

            assertNotNull(result, "Expected lookup payload after cross-origin redirect");
            assertEquals(result.getBrokerUrl(), "pulsar://broker-b:6650");
            assertEquals(result.getHttpUrl(), "http://broker-b:8080");

            // Lock the invariant: the final hop on serverB must carry the Authorization header.
            serverB.verify(getRequestedFor(urlPathMatching(LOOKUP_PATH))
                    .withHeader("Authorization", equalTo("Bearer test-token")));
        }
    }
}
