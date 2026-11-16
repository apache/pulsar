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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

/**
 * A scalable topic whose name ends with {@code -partition-<index>} can never work: {@link TopicName}
 * parses that suffix as a partition index in every domain, so each of its {@code segment://} topics
 * would be loaded as a partition of a non-existent partitioned topic. Creation must reject such names
 * up front instead of writing the metadata and then failing to create the segments.
 */
public class ScalableTopicCreateNameValidationTest extends SharedPulsarBaseTest {

    private String scalableTopic(String localNamePrefix, String suffix) {
        return "topic://" + getNamespace() + "/" + localNamePrefix + "-"
                + UUID.randomUUID().toString().substring(0, 8) + suffix;
    }

    @Test
    public void testCreateRejectsPartitionSuffixedName() throws Exception {
        String topic = scalableTopic("x", "-partition-0");

        PulsarAdminException.PreconditionFailedException e = expectThrows(
                PulsarAdminException.PreconditionFailedException.class,
                () -> admin.scalableTopics().createScalableTopic(topic, 1));
        assertThat(e.getMessage()).contains("-partition-");
        // Nothing is left behind.
        assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.scalableTopics().getMetadata(topic));
    }

    @Test
    public void testServiceCreateRejectsPartitionSuffixedName() throws Exception {
        TopicName topic = TopicName.get(scalableTopic("y", "-partition-1"));

        ExecutionException e = expectThrows(ExecutionException.class,
                () -> getPulsar().getBrokerService().getScalableTopicService()
                        .createScalableTopic(topic, 1).get());
        assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-partition-");
        assertThat(getPulsar().getPulsarResources().getScalableTopicResources()
                .getScalableTopicMetadataAsync(topic).get()).isEmpty();
    }

    @Test
    public void testCreateAllowsNameContainingPartitionWithoutIndexSuffix() throws Exception {
        // Only a trailing -partition-<index> is parsed as a partition; other uses of the word are fine.
        String topic = scalableTopic("orders-partition-archive", "");

        admin.scalableTopics().createScalableTopic(topic, 1);

        assertEquals(admin.scalableTopics().getMetadata(topic).getSegments().size(), 1);
    }
}
