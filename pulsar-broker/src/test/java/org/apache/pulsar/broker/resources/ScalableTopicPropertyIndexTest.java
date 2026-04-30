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
package org.apache.pulsar.broker.resources;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Coverage for the {@code findScalableTopicsByPropertyAsync} entry point on
 * {@link ScalableTopicResources}: verifies that topic properties registered at
 * create/update time are queryable via the secondary-index API, that updates
 * refresh the index, and that the filter is correctly scoped to a namespace.
 *
 * <p>Uses {@link LocalMemoryMetadataStore} which does not implement native
 * secondary indexes, so this exercises the fallback scan + per-record property
 * predicate path. The Oxia-native path (where the index is consulted directly)
 * is covered by the metadata-store-level secondary index tests.
 */
public class ScalableTopicPropertyIndexTest {

    private MetadataStoreExtended store;
    private ScalableTopicResources resources;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().build());
        resources = new ScalableTopicResources(store, 30);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    private TopicName topicIn(NamespaceName ns, String localName) {
        return TopicName.get("topic://" + ns + "/" + localName);
    }

    private ScalableTopicMetadata metaWithProps(Map<String, String> props) {
        return ScalableTopicMetadata.builder()
                .epoch(0)
                .nextSegmentId(1)
                .properties(new HashMap<>(props))
                .build();
    }

    @Test
    public void findsTopicByExactPropertyValue() throws Exception {
        NamespaceName ns = NamespaceName.get("tenant/ns-a");

        resources.createScalableTopicAsync(topicIn(ns, "t-alice"),
                metaWithProps(Map.of("owner", "alice", "team", "platform"))).get();
        resources.createScalableTopicAsync(topicIn(ns, "t-bob"),
                metaWithProps(Map.of("owner", "bob", "team", "platform"))).get();
        resources.createScalableTopicAsync(topicIn(ns, "t-carol"),
                metaWithProps(Map.of("owner", "carol", "team", "data"))).get();

        // Filter by owner=alice — only t-alice matches.
        List<String> aliceOwned = resources
                .findScalableTopicsByPropertyAsync(ns, "owner", "alice").get();
        assertEquals(aliceOwned, List.of("topic://tenant/ns-a/t-alice"));

        // Filter by team=platform — both alice and bob.
        Set<String> platform = new HashSet<>(resources
                .findScalableTopicsByPropertyAsync(ns, "team", "platform").get());
        assertEquals(platform, Set.of(
                "topic://tenant/ns-a/t-alice",
                "topic://tenant/ns-a/t-bob"));
    }

    @Test
    public void findIsScopedToNamespace() throws Exception {
        NamespaceName nsA = NamespaceName.get("tenant/ns-a");
        NamespaceName nsB = NamespaceName.get("tenant/ns-b");

        // Same property in two namespaces — find must return only the one we asked for.
        resources.createScalableTopicAsync(topicIn(nsA, "t1"),
                metaWithProps(Map.of("owner", "alice"))).get();
        resources.createScalableTopicAsync(topicIn(nsB, "t2"),
                metaWithProps(Map.of("owner", "alice"))).get();

        List<String> inNsA = resources
                .findScalableTopicsByPropertyAsync(nsA, "owner", "alice").get();
        assertEquals(inNsA, List.of("topic://tenant/ns-a/t1"));

        List<String> inNsB = resources
                .findScalableTopicsByPropertyAsync(nsB, "owner", "alice").get();
        assertEquals(inNsB, List.of("topic://tenant/ns-b/t2"));
    }

    @Test
    public void noMatchReturnsEmptyList() throws Exception {
        NamespaceName ns = NamespaceName.get("tenant/ns-empty");
        resources.createScalableTopicAsync(topicIn(ns, "t1"),
                metaWithProps(Map.of("owner", "alice"))).get();

        // Wrong value
        assertTrue(resources.findScalableTopicsByPropertyAsync(ns, "owner", "bob")
                .get().isEmpty());

        // Wrong key
        assertTrue(resources.findScalableTopicsByPropertyAsync(ns, "team", "alice")
                .get().isEmpty());
    }

    @Test
    public void updateRefreshesIndex() throws Exception {
        NamespaceName ns = NamespaceName.get("tenant/ns-update");
        TopicName tn = topicIn(ns, "t-mutating");

        resources.createScalableTopicAsync(tn,
                metaWithProps(Map.of("owner", "alice"))).get();
        assertEquals(resources.findScalableTopicsByPropertyAsync(ns, "owner", "alice").get(),
                List.of(tn.toString()));

        // Reassign owner via update — the new owner must be queryable, and the
        // old owner's entry must no longer match this topic.
        resources.updateScalableTopicAsync(tn, current -> {
            current.getProperties().put("owner", "bob");
            return current;
        }).get();

        assertEquals(resources.findScalableTopicsByPropertyAsync(ns, "owner", "bob").get(),
                List.of(tn.toString()));
        assertTrue(resources.findScalableTopicsByPropertyAsync(ns, "owner", "alice")
                .get().isEmpty());
    }

    @Test
    public void topicWithoutPropertiesIsNotMatched() throws Exception {
        NamespaceName ns = NamespaceName.get("tenant/ns-noprops");
        resources.createScalableTopicAsync(topicIn(ns, "t-anon"),
                metaWithProps(Map.of())).get();
        resources.createScalableTopicAsync(topicIn(ns, "t-tagged"),
                metaWithProps(Map.of("owner", "alice"))).get();

        // Filtering by any property must skip the un-tagged record.
        List<String> matches = resources
                .findScalableTopicsByPropertyAsync(ns, "owner", "alice").get();
        assertEquals(matches, List.of("topic://tenant/ns-noprops/t-tagged"));
    }
}
