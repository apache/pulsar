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
