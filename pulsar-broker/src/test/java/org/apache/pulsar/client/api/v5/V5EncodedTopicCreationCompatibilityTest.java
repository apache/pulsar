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
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.moreThanOrExactly;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.expectThrows;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Creation compatibility checks use real admin HTTP and V5 traffic, with only the owner's HTTP failure injected. */
public class V5EncodedTopicCreationCompatibilityTest {
    private PulsarTestContext context;
    private PulsarService pulsar;
    private PulsarAdmin admin;
    private PulsarAdmin internalAdmin;
    private PulsarClient client;
    private WireMockServer owner;
    private static final String NAMESPACE = "review/create-failure";

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        context = PulsarTestContext.builder().spyByDefault().build();
        pulsar = context.getPulsarService();
        admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build();
        String cluster = pulsar.getConfiguration().getClusterName();
        admin.clusters().createCluster(cluster, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("review", TenantInfo.builder().adminRoles(Set.of())
                .allowedClusters(Set.of(cluster)).build());
        admin.namespaces().createNamespace(NAMESPACE);
        admin.namespaces().setAutoTopicCreation(NAMESPACE,
                AutoTopicCreationOverride.builder().allowAutoTopicCreation(false).build());
        owner = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        owner.start();
        useUpgradedOwner();
        internalAdmin = PulsarAdmin.builder().serviceHttpUrl(owner.baseUrl()).build();
        doReturn(internalAdmin).when(pulsar).getAdminClient();
        client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        @Cleanup PulsarTestContext closingContext = context;
        @Cleanup PulsarAdmin closingAdmin = admin;
        @Cleanup PulsarAdmin closingInternalAdmin = internalAdmin;
        @Cleanup PulsarClient closingClient = client;
        if (owner != null) {
            owner.stop();
        }
    }

    @DataProvider
    public Object[][] encodedNames() {
        return new Object[][] {{"a+b"}, {"x%20y"}};
    }

    @Test(dataProvider = "encodedNames", timeOut = 60000)
    public void testUnsupportedOwnerRejectsBeforeMetadataCommit(String localName) throws Exception {
        owner.stubFor(any(urlMatching("/admin/v3/segments/.*")).atPriority(1)
                .willReturn(aResponse().withStatus(404).withHeader("Content-Type", "application/json")
                        .withBody("{\"reason\":\"v3 segment resource unavailable\"}")));
        // One initial owner supports v3; the other does not. No child may be created until all checks pass.
        owner.stubFor(get(urlMatching("/admin/v3/segments/.*/0000-7fff-0/check")).atPriority(0)
                .willReturn(aResponse().proxiedFrom(pulsar.getWebServiceAddress())));
        TopicName topic = TopicName.get("topic://" + NAMESPACE + "/" + localName);
        for (int attempt = 0; attempt < 2; attempt++) {
            assertStatus(404, () -> admin.scalableTopics().createScalableTopic(topic.toString(), 2,
                    Map.of("owner", "creation-test")));
            assertThat(pulsar.getPulsarResources().getScalableTopicResources()
                    .getScalableTopicMetadataAsync(topic, true).get(10, TimeUnit.SECONDS))
                    .as("an unsupported owner must not leave committed parent metadata").isEmpty();
        }
        owner.verify(0, putRequestedFor(urlMatching("/admin/v3/segments/.*")));
        owner.verify(4, getRequestedFor(urlMatching("/admin/v3/segments/.*/check")));

        // The same admin operation succeeds after the owner upgrades, without deleting metadata.
        useUpgradedOwner();
        admin.scalableTopics().createScalableTopic(topic.toString(), 2, Map.of("owner", "creation-test"));
        var metadata = admin.scalableTopics().getMetadata(topic.toString());
        assertThat(metadata.getProperties()).containsEntry("owner", "creation-test");
        assertThat(metadata.getSegments()).hasSize(2);
        for (var segment : metadata.getSegments().values()) {
            String name = SegmentTopicName.fromParent(topic,
                    HashRange.of(segment.getHashRange().getStart(), segment.getHashRange().getEnd()),
                    segment.getSegmentId()).toString();
            assertThat(pulsar.getBrokerService().getTopicReference(name)).as("canonical initial segment").isPresent();
        }
        assertProduceAndConsume(topic);

        // Already committed topics remain 409 even if a later owner cannot answer the preflight.
        owner.stubFor(any(urlMatching("/admin/v3/segments/.*/check")).atPriority(1)
                .willReturn(aResponse().withStatus(404)));
        assertStatus(409, () -> admin.scalableTopics().createScalableTopic(topic.toString(), 1));
    }

    @Test(timeOut = 60000)
    public void testFailureAfterPreflightKeepsRecoverableCommittedMetadata() throws Exception {
        // Ownership or availability can change after a successful check. Keep the metadata-first
        // commit model in this case: a connect can heal an ACTIVE segment without losing written data.
        owner.stubFor(put(urlMatching("/admin/v3/segments/.*")).atPriority(1)
                .willReturn(aResponse().withStatus(404).withHeader("Content-Type", "application/json")
                        .withBody("{\"reason\":\"owner changed after the compatibility check\"}")));
        TopicName topic = TopicName.get("topic://" + NAMESPACE + "/late+failure");
        TopicName segment = SegmentTopicName.fromParent(topic, HashRange.full(), 0);
        assertStatus(404, () -> admin.scalableTopics().createScalableTopic(topic.toString(), 1));
        assertThat(pulsar.getPulsarResources().getScalableTopicResources()
                .getScalableTopicMetadataAsync(topic, true).get(10, TimeUnit.SECONDS)).isPresent();
        assertThat(pulsar.getBrokerService().getTopicReference(segment.toString())).isEmpty();
        assertStatus(409, () -> admin.scalableTopics().createScalableTopic(topic.toString(), 1));

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic.toString()).create();
        producer.async().newMessage().key("key").value("recovered-without-cleanup").send()
                .get(10, TimeUnit.SECONDS);
        assertThat(pulsar.getBrokerService().getTopicReference(segment.toString())).isPresent();
        owner.verify(moreThanOrExactly(1), getRequestedFor(urlMatching("/admin/v3/segments/.*/check")));
        useUpgradedOwner();
        assertConsume(topic, "recovered-without-cleanup");
    }

    private void useUpgradedOwner() {
        owner.resetMappings();
        owner.stubFor(any(urlMatching(".*")).atPriority(10)
                .willReturn(aResponse().proxiedFrom(pulsar.getWebServiceAddress())));
    }

    private void assertProduceAndConsume(TopicName topic) throws Exception {
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic.toString()).create();
        producer.async().newMessage().key("key").value("after-upgrade").send().get(10, TimeUnit.SECONDS);
        assertConsume(topic, "after-upgrade");
    }

    private void assertConsume(TopicName topic, String expected) throws Exception {
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic.toString()).subscriptionName("review-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST).subscribe();
        Message<String> message = consumer.receive(Duration.ofSeconds(10));
        assertThat(message).isNotNull();
        assertThat(message.value()).isEqualTo(expected);
        consumer.acknowledge(message.id());
    }

    private static void assertStatus(int status, ThrowingRunnable request) {
        PulsarAdminException failure = expectThrows(PulsarAdminException.class, request::run);
        assertThat(failure.getStatusCode()).isEqualTo(status);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
