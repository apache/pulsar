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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

/**
 * Pins the JSON shape of {@link ScalableTopicStats} and {@link SegmentTopicStats} as the
 * admin REST layer and the CLI render it: the exact keys per object, helper properties that
 * must not leak, and optional members that are omitted rather than printed as empty or null.
 */
public class ScalableTopicStatsJsonTest {

    private static final ObjectMapper MAPPER = ObjectMapperFactory.getMapper().getObjectMapper();

    private static JsonNode toJson(Object value) throws Exception {
        return MAPPER.readTree(MAPPER.writeValueAsString(value));
    }

    private static Set<String> keys(JsonNode node) {
        Set<String> keys = new HashSet<>();
        node.fieldNames().forEachRemaining(keys::add);
        return keys;
    }

    @Test
    public void testTopicStatsKeys() throws Exception {
        JsonNode json = toJson(new ScalableTopicStats());

        assertEquals(keys(json), Set.of("layout", "msgRateIn", "byteRateIn", "msgRateOut", "byteRateOut",
                "averageMsgSize", "storageSize", "backlogSize", "producers", "subscriptions"));
        assertEquals(keys(json.get("layout")), Set.of("epoch", "segments"));
        assertTrue(json.get("producers").isArray());
        assertTrue(json.get("subscriptions").isObject());
    }

    @Test
    public void testLayoutSegmentOmitsHelpersAndEmptyEdges() throws Exception {
        ScalableTopicStats stats = new ScalableTopicStats();
        ScalableTopicStats.LayoutSegment parent = new ScalableTopicStats.LayoutSegment();
        parent.setName("segment://t/ns/x/0000-ffff-0");
        parent.setState("SEALED");
        parent.setChildIds(List.of(1L, 2L));
        parent.setEntryBuckets(4);
        parent.setOwnerBroker("broker-a");
        ScalableTopicStats.LayoutSegment child = new ScalableTopicStats.LayoutSegment();
        child.setName("segment://t/ns/x/0000-7fff-1");
        child.setState("ACTIVE");
        child.setParentIds(List.of(0L));
        child.setEntryBuckets(2);
        stats.getLayout().getSegments().put(0L, parent);
        stats.getLayout().getSegments().put(1L, child);

        JsonNode segments = toJson(stats).get("layout").get("segments");

        // The ID is the map key; isActive()/isSealed() are not properties; an empty edge
        // list and an unknown owner are left out.
        assertEquals(keys(segments.get("0")), Set.of("name", "state", "childIds", "entryBuckets", "ownerBroker"));
        assertEquals(keys(segments.get("1")), Set.of("name", "state", "parentIds", "entryBuckets"));
        assertEquals(segments.get("0").get("state").asText(), "SEALED");
        assertEquals(segments.get("0").get("childIds").size(), 2);
        assertEquals(segments.get("1").get("parentIds").get(0).asLong(), 0L);
    }

    @Test
    public void testProducerSubscriptionAndConsumerKeys() throws Exception {
        ScalableTopicStats stats = new ScalableTopicStats();
        ScalableTopicStats.ProducerStats producer = new ScalableTopicStats.ProducerStats();
        producer.setProducerName("p");
        producer.setAddress("/127.0.0.1:1");
        producer.setConnectedSince("2026-09-17T00:00:00Z");
        producer.setClientVersion("v5");
        stats.getProducers().add(producer);

        ScalableTopicStats.ConsumerStats consumer = new ScalableTopicStats.ConsumerStats();
        consumer.setConsumerName("c");
        consumer.setAddress("/127.0.0.1:2");
        consumer.setConnectedSince("2026-09-17T00:00:00Z");
        consumer.setClientVersion("v5");
        ScalableTopicStats.SubscriptionStats sub = new ScalableTopicStats.SubscriptionStats();
        sub.setType(ScalableSubscriptionType.QUEUE);
        sub.getSegments().put(0L, new ScalableTopicStats.SegmentSubscriptionStats());
        sub.getConsumers().add(consumer);
        stats.getSubscriptions().put("s", sub);

        JsonNode json = toJson(stats);

        // accessMode is null here and therefore omitted; every other producer key is present.
        assertEquals(keys(json.get("producers").get(0)),
                Set.of("producerName", "msgRateIn", "byteRateIn", "averageMsgSize",
                        "address", "connectedSince", "clientVersion"));
        JsonNode s = json.get("subscriptions").get("s");
        assertEquals(keys(s), Set.of("type", "msgBacklog", "backlogSize", "unackedMessages", "msgRateOut",
                "byteRateOut", "msgRateRedeliver", "messageAckRate", "segments", "consumers"));
        assertEquals(s.get("type").asText(), "QUEUE");
        assertEquals(keys(s.get("segments").get("0")), Set.of("msgBacklog", "backlogSize", "unackedMessages",
                "msgRateOut", "byteRateOut", "consumerCount"));
        assertEquals(keys(s.get("consumers").get(0)), Set.of("consumerName", "connected", "segmentIds",
                "msgRateOut", "byteRateOut", "unackedMessages", "availablePermits",
                "address", "connectedSince", "clientVersion"));
    }

    @Test
    public void testSegmentTopicStatsKeysAndOptionalDates() throws Exception {
        SegmentTopicStats stats = new SegmentTopicStats();
        stats.setOwnerBroker("broker-a");
        // topicCreationTime deliberately unset.
        SegmentTopicStats.ProducerStats producer = new SegmentTopicStats.ProducerStats();
        producer.setProducerName("p");
        stats.getProducers().add(producer);
        SegmentTopicStats.SubscriptionStats sub = new SegmentTopicStats.SubscriptionStats();
        sub.setType("Shared");
        sub.setLastConsumedTime("2026-09-17T00:00:00Z");
        // lastAckedTime deliberately unset.
        SegmentTopicStats.ConsumerStats consumer = new SegmentTopicStats.ConsumerStats();
        consumer.setConsumerName("c");
        sub.getConsumers().add(consumer);
        stats.getSubscriptions().put("s", sub);

        JsonNode json = toJson(stats);

        assertEquals(keys(json), Set.of("ownerBroker", "msgRateIn", "byteRateIn", "msgRateOut", "byteRateOut",
                "averageMsgSize", "storageSize", "backlogSize", "offloadedStorageSize",
                "oldestBacklogMessageAgeSeconds", "waitingPublishers", "producers", "subscriptions"));
        assertFalse(json.has("topicCreationTime"), "an unknown creation time is omitted, not null");
        assertEquals(keys(json.get("producers").get(0)),
                Set.of("producerName", "msgRateIn", "byteRateIn", "averageMsgSize"));
        JsonNode s = json.get("subscriptions").get("s");
        assertEquals(keys(s), Set.of("type", "msgRateOut", "byteRateOut", "msgRateRedeliver", "messageAckRate",
                "msgBacklog", "backlogSize", "unackedMessages", "oldestBacklogMessageAgeSeconds",
                "blockedSubscriptionOnUnackedMsgs", "lastConsumedTime", "consumers"));
        assertEquals(keys(s.get("consumers").get(0)), Set.of("consumerName", "msgRateOut", "byteRateOut",
                "msgRateRedeliver", "messageAckRate", "availablePermits", "unackedMessages",
                "blockedConsumerOnUnackedMsgs"));
    }

    @Test
    public void testDeserializationFillsDefaultsForOmittedMembers() throws Exception {
        ScalableTopicStats stats = MAPPER.readValue(
                "{\"layout\":{\"epoch\":7,\"segments\":{\"3\":{\"name\":\"segment://t/ns/x/0000-ffff-3\","
                        + "\"state\":\"ACTIVE\",\"entryBuckets\":1}}},\"byteRateIn\":1.5,\"unknownKey\":true}",
                ScalableTopicStats.class);

        assertEquals(stats.getLayout().getEpoch(), 7L);
        ScalableTopicStats.LayoutSegment segment = stats.getLayout().getSegments().get(3L);
        assertNotNull(segment);
        assertTrue(segment.isActive());
        assertNotNull(segment.getParentIds(), "omitted edges deserialize as empty lists");
        assertTrue(segment.getParentIds().isEmpty());
        assertTrue(segment.getChildIds().isEmpty());
        assertNull(segment.getOwnerBroker());
        assertEquals(stats.getByteRateIn(), 1.5);

        SegmentTopicStats segmentStats = MAPPER.readValue(
                "{\"subscriptions\":{\"s\":{\"type\":\"Shared\"}}}", SegmentTopicStats.class);
        SegmentTopicStats.SubscriptionStats sub = segmentStats.getSubscriptions().get("s");
        assertEquals(sub.getType(), "Shared");
        assertNull(sub.getLastAckedTime());
        assertTrue(sub.getConsumers().isEmpty());
        assertNull(segmentStats.getTopicCreationTime());
    }
}
