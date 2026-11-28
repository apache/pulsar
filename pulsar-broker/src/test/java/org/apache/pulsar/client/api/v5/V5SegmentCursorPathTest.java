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
package org.apache.pulsar.client.api.v5;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.Test;

public class V5SegmentCursorPathTest {
    @Test(timeOut = 60000)
    public void testCursorMovesThroughLegacyOwnerPath() throws Exception {
        @Cleanup
        PulsarTestContext context = PulsarTestContext.builder().spyByDefault().build();
        var pulsar = context.getPulsarService();
        String web = pulsar.getWebServiceAddress();
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(web).build();
        String cluster = pulsar.getConfiguration().getClusterName();
        admin.clusters().createCluster(cluster, ClusterData.builder().serviceUrl(web).build());
        admin.tenants().createTenant("t", TenantInfo.builder().adminRoles(Set.of())
                .allowedClusters(Set.of(cluster)).build());
        admin.namespaces().createNamespace("t/ns");
        String topic = "topic://t/ns/orders:eu";
        admin.scalableTopics().createScalableTopic(topic, 1);
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.QUEUE);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        producer.newMessage().value("one").send();
        var segment = pulsar.getBrokerService().getTopicReference("segment://t/ns/orders:eu/0000-ffff-0").orElseThrow();
        var cursor = segment.getSubscription("sub");
        assertThat(cursor.getNumberOfEntriesInBacklog(true)).isEqualTo(1);

        // Accept only the two paths that identify this segment on a 5.0.x owner. Forward to a real cursor.
        WireMockServer owner = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        owner.start();
        try {
            String path = "/admin/v2/segments/t/ns/orders:eu/0000-ffff-0/subscription/sub/";
            for (String operation : new String[] {"skip-all", "seek"}) {
                owner.stubFor(post(urlPathEqualTo(path + operation))
                        .willReturn(aResponse().proxiedFrom(web)));
            }
            @Cleanup
            PulsarAdmin internal = PulsarAdmin.builder().serviceHttpUrl(owner.baseUrl()).build();
            doReturn(internal).when(pulsar).getAdminClient();
            admin.scalableTopics().clearBacklog(topic, "sub");
            assertThat(cursor.getNumberOfEntriesInBacklog(true)).isZero();
            admin.scalableTopics().seekSubscription(topic, "sub", 0);
            assertThat(cursor.getNumberOfEntriesInBacklog(true)).isEqualTo(1);
        } finally {
            owner.stop();
        }
    }
}
