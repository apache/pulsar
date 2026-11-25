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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.apache.pulsar.common.scalable.SegmentLoadStats;
import org.testng.annotations.Test;

/**
 * The admin operations of a scalable topic whose node holds no record: what a write under the topic leaves after
 * the topic is deleted, as the store creates the missing parents of the node it writes. On Oxia the node is an empty
 * record, which the topic's metadata can't be read from.
 */
public class V5MultiBrokerScalableTopicNodeWithoutRecordTest extends V5MultiBrokerClientBaseTest {

    @Test
    public void getMetadataIsNotFound() throws Exception {
        String topic = newScalableTopic(1);
        deleteThenReportSegmentLoad(topic, 0);

        assertThatThrownBy(() -> admin.scalableTopics().getMetadata(topic))
                .as("metadata of a scalable topic whose node holds no record")
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
        assertThatThrownBy(() -> admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build()))
                .as("setting the auto split/merge policy of a scalable topic whose node holds no record")
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
    }

    @Test
    public void deleteRemovesNode() throws Exception {
        String topic = newScalableTopic(1);
        TopicName tn = deleteThenReportSegmentLoad(topic, 0);

        admin.scalableTopics().deleteScalableTopic(topic);

        assertThat(resources().getStore().exists(resources().topicPath(tn)).get())
                .as("node of the scalable topic after the delete").isFalse();
        assertThat(admin.scalableTopics().listScalableTopics(getNamespace())).doesNotContain(topic);
        assertThatThrownBy(() -> admin.scalableTopics().deleteScalableTopic(topic))
                .as("deleting the scalable topic again")
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
    }

    @Test
    public void createReplacesNode() throws Exception {
        String topic = newScalableTopic(2);
        TopicName tn = deleteThenReportSegmentLoad(topic, 1);

        admin.scalableTopics().createScalableTopic(topic, 1);

        ScalableTopicMetadata metadata = admin.scalableTopics().getMetadata(topic);
        assertThat(metadata.getSegments()).as("segments of the topic created again").containsOnlyKeys(0L);
        assertThat(resources().getStore().exists(resources().segmentLoadPath(tn, 1)).get())
                .as("load of a segment of the deleted topic, under the node the create replaced").isFalse();
        assertThat(admin.scalableTopics().listScalableTopics(getNamespace())).containsOnlyOnce(topic);
    }

    /**
     * Delete a scalable topic, then write the load of one of its segments, as the broker that owned the segment
     * reports it when the report races the delete: the write creates the node of the topic again, with no record.
     * The delete removes the segment topics too, so no broker writes their load afterwards.
     */
    private TopicName deleteThenReportSegmentLoad(String topic, long segmentId) throws Exception {
        TopicName tn = TopicName.get(topic);
        admin.scalableTopics().deleteScalableTopic(topic, true);
        resources().reportSegmentLoadAsync(tn, segmentId, new SegmentLoadStats(1, 100, 0, 0)).get();
        assertThat(resources().getStore().get(resources().topicPath(tn)).get())
                .as("node of the deleted topic %s, created again by the load report", topic)
                .hasValueSatisfying(result -> assertThat(result.getValue()).isEmpty());
        return tn;
    }

    private ScalableTopicResources resources() {
        return brokers.get(0).getPulsarResources().getScalableTopicResources();
    }
}
