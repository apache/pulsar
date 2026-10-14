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
package org.apache.pulsar.client.admin.internal;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.expectThrows;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalableTopicsEncodingCompatibilityTest {
    private WireMockServer legacyBroker;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        legacyBroker = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        legacyBroker.start();
        // Older brokers only expose v2. Accept mutations there so an accidental v2 request
        // fails the test instead of being concealed by another 404.
        legacyBroker.stubFor(put(urlMatching("/admin/v2/.*")).willReturn(aResponse().withStatus(204)));
        legacyBroker.stubFor(get(urlMatching("/admin/v2/.*")).willReturn(aResponse().withStatus(200)
                .withHeader("Content-Type", "application/json").withBody("{}")));
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (legacyBroker != null) {
            legacyBroker.stop();
        }
    }

    @DataProvider
    public Object[][] encodedTopicNames() {
        return new Object[][] {{"a b"}, {"a+b"}, {"café"}, {"x%20y"}, {"nested/topic"}};
    }

    @Test(dataProvider = "encodedTopicNames")
    public void testEncodedNamesCannotMutateLegacyBroker(String localName) throws Exception {
        TopicName topic = TopicName.get("topic://tenant/ns/" + localName);
        String segment = SegmentTopicName.fromParent(topic, HashRange.full(), 0).toString();
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(legacyBroker.baseUrl()).build();

        expectThrows(NotFoundException.class, () -> admin.scalableTopics().createScalableTopic(topic.toString(), 1));
        expectThrows(NotFoundException.class, () -> admin.scalableTopics().getMetadata(topic.toString()));
        expectThrows(NotFoundException.class, () -> admin.scalableTopics().createSegment(segment, null));
        legacyBroker.verify(0, anyRequestedFor(urlMatching("/admin/v2/.*")));
    }

    @Test
    public void testPlainNamesRemainCompatibleWithLegacyBroker() throws Exception {
        TopicName topic = TopicName.get("topic://tenant/ns/plain-topic");
        String segment = SegmentTopicName.fromParent(topic, HashRange.full(), 0).toString();
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(legacyBroker.baseUrl()).build();

        admin.scalableTopics().createScalableTopic(topic.toString(), 1);
        admin.scalableTopics().getMetadata(topic.toString());
        admin.scalableTopics().createSegment(segment, null);
        legacyBroker.verify(0, anyRequestedFor(urlMatching("/admin/v3/.*")));
        legacyBroker.verify(1, putRequestedFor(urlPathEqualTo("/admin/v2/scalable/tenant/ns/plain-topic")));
        legacyBroker.verify(1, putRequestedFor(urlPathEqualTo(
                "/admin/v2/segments/tenant/ns/plain-topic/" + TopicName.get(segment).getSegmentDescriptor())));
    }

    @Test(dataProvider = "encodedTopicNames")
    public void testRedirectToLegacySegmentOwnerCannotChangeParent(String localName) throws Exception {
        TopicName topic = TopicName.get("topic://tenant/ns/" + localName);
        String segment = SegmentTopicName.fromParent(topic, HashRange.full(), 0).toString();
        String suffix = "/segments/tenant/ns/" + topic.getEncodedLocalName()
                + "/" + TopicName.get(segment).getSegmentDescriptor();
        WireMockServer controllerBroker = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        controllerBroker.start();
        try {
            for (String version : new String[] {"v2", "v3"}) {
                String path = "/admin/" + version + suffix;
                controllerBroker.stubFor(put(urlPathEqualTo(path)).willReturn(aResponse().withStatus(307)
                        .withHeader("Location", legacyBroker.baseUrl() + path)));
            }
            String checkPath = "/admin/v3" + suffix + "/check";
            controllerBroker.stubFor(get(urlPathEqualTo(checkPath)).willReturn(aResponse().withStatus(307)
                    .withHeader("Location", legacyBroker.baseUrl() + checkPath)));
            @Cleanup
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(controllerBroker.baseUrl()).build();

            expectThrows(NotFoundException.class, () -> admin.scalableTopics().createSegment(segment, null));
            assertThat(admin.scalableTopics().checkSegmentOwnerAsync(segment)).failsWithin(3, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class).withCauseInstanceOf(NotFoundException.class);
            controllerBroker.verify(1, putRequestedFor(urlPathEqualTo("/admin/v3" + suffix)));
            legacyBroker.verify(1, putRequestedFor(urlPathEqualTo("/admin/v3" + suffix)));
            controllerBroker.verify(1, getRequestedFor(urlPathEqualTo(checkPath)));
            legacyBroker.verify(1, getRequestedFor(urlPathEqualTo(checkPath)));
            legacyBroker.verify(0, anyRequestedFor(urlMatching("/admin/v2/.*")));
        } finally {
            controllerBroker.stop();
        }
    }
}
