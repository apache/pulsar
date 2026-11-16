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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentLoadStats;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The node of a scalable topic that holds no record: what a write under the topic leaves after the topic is deleted,
 * as the store creates the missing parents of the node it writes (an empty container node on ZooKeeper), or, on the
 * stores that don't create them, like the in-memory one, the nodes under the topic's path. It is no topic: it reads
 * as absent, and a delete of the topic, or a creation of it, removes it. The admin operations on it are covered on
 * Oxia by V5MultiBrokerScalableTopicNodeWithoutRecordTest.
 */
public class ScalableTopicNodeWithoutRecordTest {

    private static final SegmentLoadStats LOAD = new SegmentLoadStats(1, 100, 0, 0);

    private TestZKServer zks;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        zks = new TestZKServer();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        if (zks != null) {
            zks.close();
            zks = null;
        }
    }

    @DataProvider
    public Object[][] stores() {
        return new Object[][]{{"zookeeper"}, {"memory"}};
    }

    @Test(dataProvider = "stores")
    public void testNodeWithoutRecordReadsAsAbsent(String storeType) throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore(storeType);
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        TopicName tn = leaveNodeWithoutRecord(resources, 0);

        assertThat(resources.getScalableTopicMetadataAsync(tn).get()).as("cached read").isEmpty();
        assertThat(resources.getScalableTopicMetadataAsync(tn, true).get()).as("read from the store").isEmpty();
        assertThatThrownBy(() -> resources.updateScalableTopicAsync(tn, metadata -> metadata).get())
                .hasCauseInstanceOf(MetadataStoreException.NotFoundException.class);
    }

    @Test(dataProvider = "stores")
    public void testDeleteRemovesNodeWithoutRecord(String storeType) throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore(storeType);
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        TopicName tn = leaveNodeWithoutRecord(resources, 0);

        assertThat(resources.deleteScalableTopicWithoutRecordAsync(tn).get()).as("node deleted").isTrue();

        assertThat(store.getChildrenFromStore(resources.namespacePath(tn.getNamespaceObject())).get())
                .as("topics of the namespace").doesNotContain(tn.getEncodedLocalName());
        assertThat(store.exists(resources.segmentLoadPath(tn, 0)).get()).as("segment load").isFalse();
        assertThat(resources.deleteScalableTopicWithoutRecordAsync(tn).get()).as("node deleted again").isFalse();
    }

    @Test(dataProvider = "stores")
    public void testCreateReplacesNodeWithoutRecord(String storeType) throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore(storeType);
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        TopicName tn = leaveNodeWithoutRecord(resources, 1);

        create(resources, tn);

        assertThat(resources.getScalableTopicMetadataAsync(tn, true).get()).as("record of the topic").isPresent();
        assertThat(store.exists(resources.segmentLoadPath(tn, 1)).get())
                .as("load of a segment of the deleted topic").isFalse();
    }

    @Test(dataProvider = "stores")
    public void testTopicWithRecordIsKept(String storeType) throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore(storeType);
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        TopicName tn = newTopic();
        create(resources, tn);
        resources.reportSegmentLoadAsync(tn, 0, LOAD).get();

        assertThat(resources.deleteScalableTopicWithoutRecordAsync(tn).get()).as("node deleted").isFalse();
        assertThatThrownBy(() -> create(resources, tn))
                .hasCauseInstanceOf(MetadataStoreException.AlreadyExistsException.class);

        assertThat(resources.getScalableTopicMetadataAsync(tn, true).get()).as("record of the topic").isPresent();
        assertThat(store.exists(resources.segmentLoadPath(tn, 0)).get()).as("segment load").isTrue();
    }

    private MetadataStoreExtended newStore(String storeType) throws Exception {
        MetadataStoreConfig config = MetadataStoreConfig.builder().build();
        return storeType.equals("zookeeper")
                ? MetadataStoreExtended.create(zks.getConnectionString(), config)
                : new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(), config);
    }

    private static TopicName newTopic() {
        return TopicName.get("topic://tenant/ns/topic-" + UUID.randomUUID().toString().substring(0, 8));
    }

    /** Write the load of a segment of a scalable topic that has no record, as a report racing its delete does. */
    private static TopicName leaveNodeWithoutRecord(ScalableTopicResources resources, long segmentId)
            throws Exception {
        TopicName tn = newTopic();
        resources.reportSegmentLoadAsync(tn, segmentId, LOAD).get();
        return tn;
    }

    private static void create(ScalableTopicResources resources, TopicName tn) throws Exception {
        resources.runWithCreationLock(tn,
                        __ -> resources.createScalableTopicAsync(tn, ScalableTopicMetadata.builder().build()))
                .get();
    }
}
