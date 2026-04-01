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
package org.apache.pulsar.common.naming;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.util.Codec;
import org.testng.annotations.Test;

public class TopicNameTest {

    @Test
    public void topic() {
        try {
            TopicName.get("://tenant.namespace:topic").getNamespace();
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // V2 format: persistent://tenant/namespace/topic
        assertEquals(TopicName.get("persistent://tenant/namespace/topic").getNamespace(),
                "tenant/namespace");

        assertEquals(TopicName.get("persistent://tenant/namespace/topic"),
                TopicName.get("persistent", "tenant", "namespace", "topic"));

        assertEquals(TopicName.get("persistent://tenant/namespace/topic").hashCode(),
                TopicName.get("persistent", "tenant", "namespace", "topic").hashCode());

        assertEquals(TopicName.get("persistent://tenant/namespace/topic").toString(),
                "persistent://tenant/namespace/topic");
        assertEquals(TopicName.toFullTopicName("persistent://tenant/namespace/topic"),
                "persistent://tenant/namespace/topic");

        assertEquals(TopicName.get("persistent://tenant/namespace/topic").getDomain(),
                TopicDomain.persistent);
        assertEquals(TopicName.get("persistent://tenant/namespace/topic").getTenant(),
                "tenant");
        assertEquals(TopicName.get("persistent://tenant/namespace/topic").getNamespacePortion(),
                "namespace");
        assertEquals(TopicName.get("persistent://tenant/namespace/topic").getNamespace(),
                "tenant/namespace");
        assertEquals(TopicName.get("persistent://tenant/namespace/topic").getLocalName(),
                "topic");

        try {
            TopicName.get("://tenant.namespace:my-topic").getDomain();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            TopicName.get("://tenant.namespace:my-topic").getTenant();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            TopicName.get("://tenant.namespace:my-topic").getNamespacePortion();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            TopicName.get("://tenant.namespace:my-topic").getLocalName();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }
        assertThrows(IllegalArgumentException.class, () -> TopicName.toFullTopicName("://tenant.namespace:my-topic"));

        try {
            TopicName.get("://tenant.namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }
        assertThrows(IllegalArgumentException.class, () -> TopicName.toFullTopicName("://tenant.namespace"));

        try {
            TopicName.get("invalid://tenant/namespace/topic");
            fail("Should have raied exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.toFullTopicName("invalid://tenant/namespace/topic"));

        // V1 topic names (with cluster component) are no longer supported
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("persistent://tenant/cluster/namespace/topic"));
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("non-persistent://tenant/cluster/namespace/topic"));

        // 4-part short topic names (without domain) are not supported
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("tenant/cluster/namespace/topic"));
        assertThrows(IllegalArgumentException.class, () -> TopicName.toFullTopicName("tenant/cluster/namespace/topic"));

        try {
            TopicName.get("persistent:///namespace/mydest-1");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.toFullTopicName("persistent:///namespace/mydest-1"));

        try {
            TopicName.get("persistent://pulsar//mydest-1");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.toFullTopicName("persistent://pulsar//mydest-1"));

        try {
            TopicName.get("://tenant.namespace");
            fail("Should have raied exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }
        assertThrows(IllegalArgumentException.class, () -> TopicName.toFullTopicName("://tenant.namespace"));

        try {
            TopicName.get(" ");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }
        assertThrows(IllegalArgumentException.class, () -> TopicName.toFullTopicName(" "));

        // V2 topic names do not allow '/' in local names (V1 did)
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("persistent://tenant/namespace/ns-abc/table/1"));
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("persistent://tenant/namespace/ns-abc/table/1/"));
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("persistent://tenant/namespace/$#3rpa/table/1"));

        TopicName topicName = TopicName.get("persistent://myprop/myns/mytopic");
        assertEquals(topicName.getPartition(0).toString(), "persistent://myprop/myns/mytopic-partition-0");

        TopicName partitionedDn = TopicName.get("persistent://myprop/myns/mytopic").getPartition(2);
        assertEquals(partitionedDn.getPartitionIndex(), 2);
        assertEquals(topicName.getPartitionIndex(), -1);

        assertEquals(TopicName.getPartitionIndex("persistent://myprop/myns/mytopic-partition-4"), 4);

        // Following behavior is not right actually, none partitioned topic, partition index is -1
        assertEquals(TopicName.getPartitionIndex("mytopic-partition--1"), -1);
        assertEquals(TopicName.getPartitionIndex("mytopic-partition-00"), -1);
        assertEquals(TopicName.getPartitionIndex("mytopic-partition-012"), -1);

        assertFalse(TopicName.get("mytopic-partition--1").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition--2").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition-01").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition-012").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition- 12").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition-12 ").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition- 12 ").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition-1&").isPartitioned());
        assertFalse(TopicName.get("mytopic-partition-1!").isPartitioned());

        assertTrue(TopicName.get("mytopic-partition-0").isPartitioned());
        assertTrue(TopicName.get("mytopic-partition-1").isPartitioned());
        assertTrue(TopicName.get("mytopic-partition-12").isPartitioned());
    }

    @Test
    public void testDecodeEncode() throws Exception {
        String encodedName = "a%3Aen-in_in_business_content_item_20150312173022_https%5C%3A";
        String rawName = "a:en-in_in_business_content_item_20150312173022_https\\:";
        assertEquals(Codec.decode(encodedName), rawName);
        assertEquals(Codec.encode(rawName), encodedName);

        String topicName = "persistent://prop/ns/" + rawName;
        TopicName name = TopicName.get(topicName);

        assertEquals(name.getLocalName(), rawName);
        assertEquals(name.getEncodedLocalName(), encodedName);
        assertEquals(name.getPersistenceNamingEncoding(), "prop/ns/persistent/" + encodedName);
    }

    @Test
    public void testFromPersistenceNamingEncoding() {
        // case1: V2 (4-part ML name: tenant/namespace/persistent/topic)
        String mlName1 = "public_tenant/default_namespace/persistent/test_topic";
        String expectedTopicName1 = "persistent://public_tenant/default_namespace/test_topic";

        TopicName name1 = TopicName.get(expectedTopicName1);
        assertEquals(name1.getPersistenceNamingEncoding(), mlName1);
        assertEquals(TopicName.fromPersistenceNamingEncoding(mlName1), expectedTopicName1);

        // case2: 5-part ML name (legacy V1 format: tenant/cluster/namespace/persistent/topic)
        // Now produces V2 output with cluster dropped
        String mlName2 = "public_tenant/my_cluster/default_namespace/persistent/test_topic";
        String expectedTopicName2 = "persistent://public_tenant/default_namespace/test_topic";
        assertEquals(TopicName.fromPersistenceNamingEncoding(mlName2), expectedTopicName2);

        // case3: empty
        String mlName3 = "";
        String expectedTopicName3 = "";
        assertEquals(expectedTopicName3, TopicName.fromPersistenceNamingEncoding(mlName3));

        // case4: Invalid name (6-part)
        try {
            String mlName4 = "public_tenant/my_cluster/default_namespace/persistent/test_topic/sub_topic";
            TopicName.fromPersistenceNamingEncoding(mlName4);
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Exception is expected.
        }

        // case5: local name with special characters e.g. a:b:c
        String topicName = "persistent://tenant/namespace/a:b:c";
        String persistentNamingEncoding = "tenant/namespace/persistent/a%3Ab%3Ac";
        assertEquals(TopicName.get(topicName).getPersistenceNamingEncoding(), persistentNamingEncoding);
        assertEquals(TopicName.fromPersistenceNamingEncoding(persistentNamingEncoding), topicName);
    }


    @Test
    public void testTopicNameProperties() throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/topic");

        assertEquals(topicName.getNamespace(), "tenant/namespace");

        assertEquals(topicName, TopicName.get("persistent", "tenant", "namespace", "topic"));

        assertEquals(topicName.hashCode(),
                TopicName.get("persistent", "tenant", "namespace", "topic").hashCode());

        assertEquals(topicName.toString(), "persistent://tenant/namespace/topic");
        assertEquals(topicName.getDomain(), TopicDomain.persistent);
        assertEquals(topicName.getTenant(), "tenant");
        assertEquals(topicName.getNamespacePortion(), "namespace");
        assertEquals(topicName.getNamespace(), "tenant/namespace");
        assertEquals(topicName.getLocalName(), "topic");

        assertEquals(topicName.getEncodedLocalName(), "topic");
        assertEquals(topicName.getPartitionedTopicName(), "persistent://tenant/namespace/topic");
        assertEquals(topicName.getPersistenceNamingEncoding(), "tenant/namespace/persistent/topic");
    }

    @Test
    public void testShortTopicName() throws Exception {
        TopicName tn = TopicName.get("short-topic");
        assertEquals(TopicDomain.persistent, tn.getDomain());
        assertEquals(TopicName.PUBLIC_TENANT, tn.getTenant());
        assertEquals(TopicName.DEFAULT_NAMESPACE, tn.getNamespacePortion());
        assertEquals("short-topic", tn.getLocalName());

        tn = TopicName.get("test-tenant/test-namespace/test-short-topic");
        assertEquals(TopicDomain.persistent, tn.getDomain());
        assertEquals("test-tenant", tn.getTenant());
        assertEquals("test-namespace", tn.getNamespacePortion());
        assertEquals("test-short-topic", tn.getLocalName());

        // 4-part V1 names are no longer supported
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("pulsar/cluster/namespace/test"));

        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("pulsar/cluster"));
    }

    @Test
    public void testTwoKeyWordPartition(){
        TopicName tp1 = TopicName.get("tenant1/namespace1/tp1-partition-0-DLQ");
        TopicName tp2 = tp1.getPartition(0);
        assertNotEquals(tp2.toString(), tp1.toString());
        assertEquals(tp2.toString(), "persistent://tenant1/namespace1/tp1-partition-0-DLQ-partition-0");
    }

    @Test
    public void testScalableTopicDomain() {
        // topic:// domain - basic parsing
        TopicName tn = TopicName.get("topic://tenant/namespace/my-topic");
        assertEquals(tn.getDomain(), TopicDomain.topic);
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "namespace");
        assertEquals(tn.getLocalName(), "my-topic");
        assertEquals(tn.toString(), "topic://tenant/namespace/my-topic");
        assertTrue(tn.isScalable());
        assertFalse(tn.isSegment());
        assertFalse(tn.getSegmentRange().isPresent());

        // topic:// equality and factory methods
        assertEquals(TopicName.get("topic://tenant/namespace/my-topic"),
                TopicName.get("topic", "tenant", "namespace", "my-topic"));

        // topic:// namespace
        assertEquals(TopicName.get("topic://t/ns/x").getNamespace(), "t/ns");

        // topic:// isValid
        assertTrue(TopicName.isValid("topic://tenant/namespace/my-topic"));
        assertTrue(TopicName.isValid("segment://tenant/namespace/my-topic/0000-ffff-0"));

        // topic:// blank local name rejected
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("topic://tenant/namespace/"));

        // topic:// missing namespace rejected
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("topic://tenant"));

        // Partitioned topic with topic:// domain
        tn = TopicName.get("topic://tenant/namespace/my-topic-partition-3");
        assertTrue(tn.isPartitioned());
        assertEquals(tn.getPartitionIndex(), 3);
        assertEquals(tn.getPartitionedTopicName(), "topic://tenant/namespace/my-topic");

        // Non-partitioned with topic:// domain
        tn = TopicName.get("topic://tenant/namespace/my-topic");
        assertFalse(tn.isPartitioned());
        assertEquals(tn.getPartitionIndex(), -1);
    }

    @Test
    public void testSegmentDomain() {
        // segment:// domain - full range
        TopicName tn = TopicName.get("segment://tenant/namespace/my-topic/0000-ffff-0");
        assertEquals(tn.getDomain(), TopicDomain.segment);
        assertTrue(tn.isSegment());
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "namespace");
        assertEquals(tn.getLocalName(), "my-topic");
        assertEquals(tn.getSegmentRange(), java.util.Optional.of(HashRange.of(0x0000, 0xFFFF)));
        assertEquals(tn.getSegmentId(), 0);
        assertEquals(tn.toString(), "segment://tenant/namespace/my-topic/0000-ffff-0");

        // segment:// with lower half-range
        tn = TopicName.get("segment://tenant/namespace/my-topic/0000-7fff-1");
        assertEquals(tn.getLocalName(), "my-topic");
        assertEquals(tn.getSegmentRange(), java.util.Optional.of(HashRange.of(0x0000, 0x7FFF)));
        assertEquals(tn.getSegmentId(), 1);

        // segment:// with upper half-range
        tn = TopicName.get("segment://tenant/namespace/my-topic/8000-ffff-2");
        assertEquals(tn.getLocalName(), "my-topic");
        assertEquals(tn.getSegmentRange(), java.util.Optional.of(HashRange.of(0x8000, 0xFFFF)));
        assertEquals(tn.getSegmentId(), 2);

        // segment:// missing descriptor rejected
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("segment://tenant/namespace/my-topic"));

        // segment:// blank local name rejected
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("segment://tenant/namespace/"));

        // segment:// invalid descriptor rejected
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.get("segment://tenant/namespace/my-topic/bad-descriptor"));
    }

    @Test
    public void testScalableTopicPersistence() {
        // topic:// and segment:// are persistent (backed by managed ledgers)
        assertTrue(TopicName.get("topic://tenant/namespace/t").isPersistent());
        assertTrue(TopicName.get("segment://tenant/namespace/t/0000-ffff-0").isPersistent());

        // persistent:// and non-persistent:// are not scalable
        assertFalse(TopicName.get("persistent://tenant/namespace/t").isScalable());
        assertFalse(TopicName.get("non-persistent://tenant/namespace/t").isScalable());

        // segment:// is not scalable (it's a segment of a scalable topic)
        assertFalse(TopicName.get("segment://tenant/namespace/t/0000-ffff-0").isScalable());
    }

    @Test
    public void testScalableTopicPersistenceNamingEncoding() {
        // topic:// persistence encoding (no domain component, no managed ledger)
        TopicName tn = TopicName.get("topic://tenant/namespace/my-topic");
        assertEquals(tn.getPersistenceNamingEncoding(), "tenant/namespace/my-topic");

        // segment:// persistence encoding includes descriptor
        tn = TopicName.get("segment://tenant/namespace/my-topic/0000-ffff-0");
        assertEquals(tn.getPersistenceNamingEncoding(), "tenant/namespace/segment/my-topic/0000-ffff-0");

        tn = TopicName.get("segment://tenant/namespace/my-topic/0000-7fff-1");
        assertEquals(tn.getPersistenceNamingEncoding(), "tenant/namespace/segment/my-topic/0000-7fff-1");

        tn = TopicName.get("segment://tenant/namespace/my-topic/8000-ffff-2");
        assertEquals(tn.getPersistenceNamingEncoding(), "tenant/namespace/segment/my-topic/8000-ffff-2");

        // segment:// with special characters in local name
        tn = TopicName.get("segment://tenant/namespace/a:b/0000-ffff-0");
        assertEquals(tn.getPersistenceNamingEncoding(), "tenant/namespace/segment/a%3Ab/0000-ffff-0");
    }

    @Test
    public void testScalableTopicFromPersistenceNamingEncoding() {
        // segment:// round-trip through persistence encoding
        String segmentTopic = "segment://tenant/namespace/my-topic/0000-ffff-0";
        TopicName tn = TopicName.get(segmentTopic);
        String mlName = tn.getPersistenceNamingEncoding();
        assertEquals(mlName, "tenant/namespace/segment/my-topic/0000-ffff-0");
        assertEquals(TopicName.fromPersistenceNamingEncoding(mlName), segmentTopic);

        // segment:// with split range
        segmentTopic = "segment://tenant/namespace/my-topic/0000-7fff-1";
        tn = TopicName.get(segmentTopic);
        mlName = tn.getPersistenceNamingEncoding();
        assertEquals(TopicName.fromPersistenceNamingEncoding(mlName), segmentTopic);

        // segment:// with special characters in local name
        segmentTopic = "segment://tenant/namespace/a:b/0000-ffff-0";
        tn = TopicName.get(segmentTopic);
        mlName = tn.getPersistenceNamingEncoding();
        assertEquals(TopicName.fromPersistenceNamingEncoding(mlName), segmentTopic);

        // topic:// persistence encoding is 3-part (no domain, no managed ledger)
        // so fromPersistenceNamingEncoding is not applicable for topic://
    }

    @Test
    public void testScalableTopicSchemaName() {
        // topic:// schema name (uses tenant/namespacePortion/encodedLocalName)
        TopicName tn = TopicName.get("topic://tenant/namespace/my-topic");
        assertEquals(tn.getSchemaName(), "tenant/namespace/my-topic");

        // segment:// schema name — should use the parent topic name, not the descriptor
        tn = TopicName.get("segment://tenant/namespace/my-topic/0000-ffff-0");
        assertEquals(tn.getSchemaName(), "tenant/namespace/my-topic");

        // Different segment ranges of the same topic share the same schema name
        TopicName seg1 = TopicName.get("segment://tenant/namespace/my-topic/0000-7fff-1");
        TopicName seg2 = TopicName.get("segment://tenant/namespace/my-topic/8000-ffff-2");
        assertEquals(seg1.getSchemaName(), seg2.getSchemaName());

        // topic:// with special characters
        tn = TopicName.get("topic://tenant/namespace/a:b");
        assertEquals(tn.getSchemaName(), "tenant/namespace/a%3Ab");
    }

    @Test
    public void testScalableTopicLookupName() {
        // topic:// lookup name
        TopicName tn = TopicName.get("topic://tenant/namespace/my-topic");
        assertEquals(tn.getLookupName(), "topic/tenant/namespace/my-topic");

        // segment:// lookup name — includes the parent topic name (not descriptor)
        tn = TopicName.get("segment://tenant/namespace/my-topic/0000-ffff-0");
        assertEquals(tn.getLookupName(), "segment/tenant/namespace/my-topic");
    }

    @Test
    public void testScalableTopicRestPath() {
        // topic:// rest path with domain
        TopicName tn = TopicName.get("topic://tenant/namespace/my-topic");
        assertEquals(tn.getRestPath(), "topic/tenant/namespace/my-topic");
        assertEquals(tn.getRestPath(false), "tenant/namespace/my-topic");

        // segment:// rest path with domain
        tn = TopicName.get("segment://tenant/namespace/my-topic/0000-ffff-0");
        assertEquals(tn.getRestPath(), "segment/tenant/namespace/my-topic");
        assertEquals(tn.getRestPath(false), "tenant/namespace/my-topic");
    }

    @Test
    public void testSegmentDescriptor() {
        // segment:// getSegmentDescriptor
        TopicName tn = TopicName.get("segment://tenant/namespace/my-topic/0000-ffff-0");
        assertEquals(tn.getSegmentDescriptor(), "0000-ffff-0");

        tn = TopicName.get("segment://tenant/namespace/my-topic/0000-7fff-1");
        assertEquals(tn.getSegmentDescriptor(), "0000-7fff-1");

        tn = TopicName.get("segment://tenant/namespace/my-topic/8000-ffff-2");
        assertEquals(tn.getSegmentDescriptor(), "8000-ffff-2");

        // Non-segment domains return null
        assertNull(TopicName.get("persistent://tenant/namespace/my-topic").getSegmentDescriptor());
        assertNull(TopicName.get("topic://tenant/namespace/my-topic").getSegmentDescriptor());
    }

    @Test
    public void testToFullTopicName() {
        // There is no constraint for local topic name
        assertEquals("persistent://public/default/tp???xx=", TopicName.toFullTopicName("tp???xx="));
        assertEquals("persistent://tenant/ns/tp???xx=", TopicName.toFullTopicName("tenant/ns/tp???xx="));
        assertEquals("persistent://tenant/ns/test", TopicName.toFullTopicName("persistent://tenant/ns/test"));
        assertThrows(IllegalArgumentException.class, () -> TopicName.toFullTopicName("ns/topic"));
        // v1 format is not supported
        assertThrows(IllegalArgumentException.class, () -> TopicName.toFullTopicName("tenant/cluster/ns/topic"));
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.toFullTopicName("persistent://tenant/cluster/ns/topic"));
        assertThrows(IllegalArgumentException.class,
                () -> TopicName.toFullTopicName("non-persistent://tenant/cluster/ns/topic"));
    }
}
