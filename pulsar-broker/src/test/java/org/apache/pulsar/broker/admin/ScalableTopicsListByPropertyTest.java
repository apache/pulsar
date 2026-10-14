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
package org.apache.pulsar.broker.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * End-to-end coverage for the property-filtered list endpoint
 * ({@code GET /admin/v2/scalable/{tenant}/{namespace}?propertyKey&propertyValue}).
 * Drives the full HTTP path through the {@link org.apache.pulsar.client.admin.PulsarAdmin}
 * client against a real shared broker, verifying that topics created with
 * properties through the admin API are queryable via the secondary index.
 */
public class ScalableTopicsListByPropertyTest extends SharedPulsarBaseTest {

    private String namespace() {
        return getNamespace();
    }

    private String topicName(String suffix) {
        return "topic://" + namespace() + "/" + suffix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @DataProvider
    public Object[][] encodedTopicNames() {
        return new Object[][] {{"a b"}, {"a+b"}, {"café"}, {"x%20y"}, {"nested/topic"}};
    }

    @DataProvider
    public Object[][] legacyTopicNames() {
        return new Object[][] {{"a+b", "a b"}, {"x%20y", "x y"}};
    }

    @Test(dataProvider = "legacyTopicNames")
    public void testLegacyMetadataPathPreservesTopicIdentity(String localName, String decodedName) throws Exception {
        String topic = topicName(localName);
        String otherTopic = topic.replace(localName, decodedName);
        admin.scalableTopics().createScalableTopic(topic, 1, Map.of("owner", "literal"));
        admin.scalableTopics().createScalableTopic(otherTopic, 1, Map.of("owner", "decoded"));

        // Match an older admin client's WebTarget.path(rawLocalName), including literal percent escapes.
        @Cleanup
        Client client = ClientBuilder.newClient();
        try (Response response = client.target(getWebServiceUrl()).path("/admin/v2/scalable")
                .path(namespace()).path(TopicName.get(topic).getLocalName())
                .request(MediaType.APPLICATION_JSON).get()) {
            assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
            assertThat(response.readEntity(String.class)).contains("literal").doesNotContain("decoded");
        }
        getPulsar().getBrokerService().getScalableTopicService().releaseController(TopicName.get(topic)).get();
        getPulsar().getBrokerService().getScalableTopicService().releaseController(TopicName.get(otherTopic)).get();
        admin.scalableTopics().deleteScalableTopic(topic, true);
        admin.scalableTopics().deleteScalableTopic(otherTopic, true);
    }

    @Test(dataProvider = "legacyTopicNames")
    public void testLegacySegmentPathPreservesParentIdentity(String localName, String decodedName) throws Exception {
        TopicName parent = TopicName.get(topicName(localName));
        TopicName segment = SegmentTopicName.fromParent(parent, HashRange.full(), 0);
        TopicName otherSegment = SegmentTopicName.fromParent(
                TopicName.get(parent.toString().replace(localName, decodedName)), HashRange.full(), 0);
        @Cleanup
        Client client = ClientBuilder.newClient();
        try (Response response = client.target(getWebServiceUrl()).path("/admin/v2/segments")
                .path(namespace()).path(parent.getLocalName()).path(segment.getSegmentDescriptor())
                .request(MediaType.APPLICATION_JSON).put(Entity.json(""))) {
            assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        }
        assertThat(getTopicIfExists(segment.toString()).get()).as("literal legacy parent").isPresent();
        assertThat(getTopicIfExists(otherSegment.toString()).get()).as("decoded parent must not be created").isEmpty();
        admin.scalableTopics().deleteSegment(segment.toString(), true);
    }

    @Test(dataProvider = "encodedTopicNames")
    public void testEncodedTopicNameRoundTrip(String localName) throws Exception {
        String topic = topicName(localName);
        admin.scalableTopics().createScalableTopic(topic, 1, Map.of("owner", "encoded-name"));

        assertThat(admin.scalableTopics().getMetadata(topic).getProperties())
                .containsEntry("owner", "encoded-name");
        assertThat(admin.scalableTopics().listScalableTopicsByProperties(namespace(),
                Map.of("owner", "encoded-name"))).containsExactly(topic);
        String segment = SegmentTopicName.fromParent(TopicName.get(topic), HashRange.full(), 0).toString();
        assertThat(getTopicIfExists(segment).get()).as("canonical initial segment").isPresent();
        assertThat(admin.scalableTopics().getStats(topic)).isNotNull();
        assertThat(admin.scalableTopics().getSegmentStats(topic, 0)).isNotNull();

        AutoScalePolicyOverride override = AutoScalePolicyOverride.builder().enabled(false).build();
        admin.scalableTopics().setAutoScalePolicy(topic, override);
        assertThat(admin.scalableTopics().getAutoScalePolicy(topic)).isEqualTo(override);
        admin.scalableTopics().removeAutoScalePolicy(topic);
        assertThat(admin.scalableTopics().getAutoScalePolicy(topic)).isNull();

        // Slash-containing parents have a separate pending-ack topic naming limitation.
        if (!localName.contains("/")) {
            admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.QUEUE);
            assertThat(admin.scalableTopics().getSegmentSubscriptionBacklogAsync(segment, "sub").get()).isZero();
            admin.scalableTopics().seekSubscription(topic, "sub", System.currentTimeMillis());
            admin.scalableTopics().clearBacklog(topic, "sub");
            admin.scalableTopics().deleteSubscription(topic, "sub");
        }
        // Quiesce the fixture controller before deletion: the memory store's child listing
        // treats '+' in an encoded parent path as a regex, failing to strip the path prefix.
        getPulsar().getBrokerService().getScalableTopicService()
                .releaseController(TopicName.get(topic)).get();
        admin.scalableTopics().deleteScalableTopic(topic, true);
        assertThat(getPulsar().getPulsarResources().getScalableTopicResources()
                .getScalableTopicMetadataAsync(TopicName.get(topic)).get()).isEmpty();
        assertThat(getTopicIfExists(segment).get()).as("deleted canonical segment").isEmpty();
    }

    @Test
    public void testLayoutChangesPreserveEncodedTopicName() throws Exception {
        String topic = topicName("layout + café%20/nested");
        admin.scalableTopics().createScalableTopic(topic, 1);
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        admin.scalableTopics().splitSegment(topic, 0);
        admin.scalableTopics().mergeSegments(topic, 1, 2);
        admin.scalableTopics().rebucketSegment(topic, 3, 8);

        var metadata = admin.scalableTopics().getMetadata(topic);
        assertThat(metadata.getSegments().get(4L).isActive()).isTrue();
        String segment = SegmentTopicName.fromParent(TopicName.get(topic), HashRange.full(), 4).toString();
        assertThat(getTopicIfExists(segment).get()).as("canonical rebucketed segment").isPresent();
        getPulsar().getBrokerService().getScalableTopicService()
                .releaseController(TopicName.get(topic)).get();
        admin.scalableTopics().deleteScalableTopic(topic, true);
        assertThat(getTopicIfExists(segment).get()).isEmpty();
    }

    @Test
    public void listScalableTopicsFilteredByProperty() throws Exception {
        String aliceTopic = topicName("alice");
        String bobTopic = topicName("bob");
        String carolTopic = topicName("carol");

        // alice and bob share team=platform; alice and carol share owner=alice. We
        // can hit each consumer-driven slice via different filter combinations below.
        admin.scalableTopics().createScalableTopic(aliceTopic, 1,
                Map.of("owner", "alice", "team", "platform"));
        admin.scalableTopics().createScalableTopic(bobTopic, 1,
                Map.of("owner", "bob", "team", "platform"));
        admin.scalableTopics().createScalableTopic(carolTopic, 1,
                Map.of("owner", "alice", "team", "data"));

        // Single-property filter: owner=bob — single match.
        List<String> bob = admin.scalableTopics()
                .listScalableTopicsByProperties(namespace(), Map.of("owner", "bob"));
        assertEquals(bob, List.of(bobTopic));

        // Single-property filter: team=platform — alice + bob.
        Set<String> platform = new HashSet<>(admin.scalableTopics()
                .listScalableTopicsByProperties(namespace(), Map.of("team", "platform")));
        assertEquals(platform, Set.of(aliceTopic, bobTopic));

        // Multi-property AND filter: owner=alice AND team=platform — narrows to
        // exactly aliceTopic, even though carol also has owner=alice and bob also
        // has team=platform.
        List<String> aliceOnPlatform = admin.scalableTopics()
                .listScalableTopicsByProperties(namespace(),
                        Map.of("owner", "alice", "team", "platform"));
        assertEquals(aliceOnPlatform, List.of(aliceTopic));

        // Unmatched combination — empty result.
        assertTrue(admin.scalableTopics()
                .listScalableTopicsByProperties(namespace(),
                        Map.of("owner", "alice", "team", "ops"))
                .isEmpty());

        // Sanity-check: the un-filtered listing still returns every topic in the namespace.
        Set<String> all = new HashSet<>(admin.scalableTopics().listScalableTopics(namespace()));
        assertTrue(all.containsAll(Set.of(aliceTopic, bobTopic, carolTopic)),
                "expected all three created topics to appear in the unfiltered list, got " + all);
    }

    /**
     * A scalable topic created with trailing whitespace could never be reached: clients trim topic names, so
     * they would look up the trimmed name instead. The admin client must reject it with a 412
     * (PreconditionFailedException) on its fast-fail path.
     */
    @Test
    public void testCreateScalableTopicWithSurroundingWhitespaceIsRejected() throws Exception {
        String topicWithWhitespace = "topic://" + namespace() + "/scalable-with-whitespace-"
                + UUID.randomUUID().toString().substring(0, 8) + " ";
        PulsarAdminException.PreconditionFailedException e = expectThrows(
                PulsarAdminException.PreconditionFailedException.class,
                () -> admin.scalableTopics().createScalableTopic(topicWithWhitespace, 1,
                        Map.of("owner", "test")));
        assertTrue(e.getMessage().contains("whitespace"), "expected the surrounding-whitespace rejection, got: "
                + e.getMessage());
    }

    /**
     * The admin client rejects trailing whitespace client-side, so a raw HTTP request is needed to exercise the
     * server. A percent-encoded trailing space must be decoded by the server and rejected with a 412.
     */
    @Test
    public void testCreateScalableTopicServerSideRejectsEncodedWhitespace() {
        NamespaceName ns = NamespaceName.get(namespace());
        String encodedTopic = "scalable-encoded-whitespace-" + UUID.randomUUID().toString().substring(0, 8) + "%20";
        String url = getWebServiceUrl() + "/admin/v2/scalable/" + ns.getTenant() + "/" + ns.getLocalName()
                + "/" + encodedTopic;
        @Cleanup
        Client client = ClientBuilder.newClient();
        try (Response response = client.target(url)
                .request(MediaType.APPLICATION_JSON)
                .put(Entity.json(Map.of("owner", "test")))) {
            assertEquals(response.getStatus(), Response.Status.PRECONDITION_FAILED.getStatusCode());
            assertTrue(response.readEntity(String.class).contains("whitespace"));
        }
    }

    /**
     * The transaction-internal-name rule is specific to persistent topics. The shared whitespace validation
     * must not extend it to non-persistent or scalable topics.
     */
    @Test
    public void testTransactionInternalSuffixIsNotRejectedForNonPersistentOrScalableTopics() throws Exception {
        String ns = namespace();
        admin.nonPersistentTopics().createPartitionedTopic(
                "non-persistent://" + ns + "/foo__transaction_pending_ack", 1);
        admin.scalableTopics().createScalableTopic(
                "topic://" + ns + "/foo__transaction_pending_ack", 1, Map.of("owner", "test"));
    }
}
