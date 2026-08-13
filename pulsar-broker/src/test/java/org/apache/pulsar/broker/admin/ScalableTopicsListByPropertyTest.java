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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
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
}
