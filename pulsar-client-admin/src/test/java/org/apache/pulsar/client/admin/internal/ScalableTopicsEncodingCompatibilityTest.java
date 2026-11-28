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
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalableTopicsEncodingCompatibilityTest {
    @DataProvider
    public Object[][] topicPaths() {
        return new Object[][] {{"orders:eu", "orders:eu"}, {"k=v", "k=v"}, {"a+b", "a+b"},
                {"a b", "a%20b"}, {"café", "caf%C3%A9"}, {"a[1]", "a%5B1%5D"}, {"a%zz", "a%25zz"},
                {"x%20y", "x%2520y"}, {"nested/topic", "nested%2Ftopic"}, {"orders;eu", "orders%3Beu"},
                {"{id}", "%7Bid%7D"}};
    }

    @Test(dataProvider = "topicPaths")
    public void testTopicAndSegmentWirePaths(String localName, String expectedPath) throws Exception {
        WireMockServer server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        server.start();
        try {
            server.stubFor(put(urlPathEqualTo("/admin/v2/scalable/t/ns/" + expectedPath))
                    .willReturn(aResponse().withStatus(204)));
            server.stubFor(put(urlPathEqualTo("/admin/v2/segments/t/ns/" + expectedPath + "/0000-ffff-0"))
                    .willReturn(aResponse().withStatus(204)));
            @Cleanup
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(server.baseUrl()).build();
            admin.scalableTopics().createScalableTopic("topic://t/ns/" + localName, 1);
            admin.scalableTopics().createSegment("segment://t/ns/" + localName + "/0000-ffff-0", null);
        } finally {
            server.stop();
        }
    }
}
