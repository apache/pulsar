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
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.netty.DnsResolverUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * End-to-end check that {@link HttpLookupService#getBroker} carries the {@code Authorization} header
 * across a cross-origin redirect — the production scenario where a broker returns
 * {@code 307 Temporary Redirect} with a {@code Location} pointing at another broker's
 * {@code httpUrl}/{@code httpUrlTls}.
 */
public class HttpLookupServiceTest {

    private static final String TOPIC = "persistent://public/default/cross-origin-lookup-topic";
    private static final String LOOKUP_PATH_REGEX =
            "/lookup/v2/topic/persistent/public/default/cross-origin-lookup-topic";
    private static final String LOOKUP_BODY = "{\"brokerUrl\":\"pulsar://broker-b.example:6650\","
            + "\"brokerUrlTls\":\"pulsar+ssl://broker-b.example:6651\","
            + "\"httpUrl\":\"http://broker-b.example:8080\","
            + "\"httpUrlTls\":\"https://broker-b.example:8443\","
            + "\"nativeUrl\":\"pulsar://broker-b.example:6650\"}";

    private WireMockServer serverA;
    private WireMockServer serverB;
    private EventLoopGroup eventLoopGroup;
    private Timer timer;

    @BeforeClass(alwaysRun = true)
    void beforeClass() {
        eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("HttpLookupServiceTest"));
        timer = new HashedWheelTimer(new DefaultThreadFactory("HttpLookupServiceTest-timer"));
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
    public void testGetBrokerFollowsCrossOriginRedirect() throws Exception {
        serverA.stubFor(get(urlPathMatching(LOOKUP_PATH_REGEX))
                .willReturn(aResponse()
                        .withStatus(307)
                        .withHeader("Location",
                                "http://127.0.0.1:" + serverB.port() + LOOKUP_PATH_REGEX)));

        serverB.stubFor(get(urlPathMatching(LOOKUP_PATH_REGEX))
                .atPriority(2)
                .willReturn(aResponse().withStatus(401).withBody("missing auth")));
        serverB.stubFor(get(urlPathMatching(LOOKUP_PATH_REGEX))
                .atPriority(1)
                .withHeader("Authorization", equalTo("Bearer test-token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(LOOKUP_BODY)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://127.0.0.1:" + serverA.port());
        conf.setAuthentication(AuthenticationFactory.token("test-token"));

        HttpLookupService lookupService = new HttpLookupService(
                InstrumentProvider.NOOP, conf, eventLoopGroup, timer, DnsResolverUtil.adaptToNameResolver(null));
        try {
            LookupTopicResult result = lookupService.getBroker(TopicName.get(TOPIC), null)
                    .get(30, TimeUnit.SECONDS);

            assertNotNull(result);
            assertEquals(result.getLogicalAddress().getHostString(), "broker-b.example");
            assertEquals(result.getLogicalAddress().getPort(), 6650);

            serverB.verify(getRequestedFor(urlPathMatching(LOOKUP_PATH_REGEX))
                    .withHeader("Authorization", equalTo("Bearer test-token")));
        } finally {
            lookupService.close();
        }
    }
}
