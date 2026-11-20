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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.stream.IntStream;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.SharedMultiBrokerPulsarBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class SubscriptionCursorInternalStatsMultiBrokerTest extends SharedMultiBrokerPulsarBaseTest {

    @Test
    public void testPartitionFanOutFollowsPhysicalRedirect() throws Exception {
        String topic = newTopicName();
        String subscription = "subscription";
        TopicName firstPartition = TopicName.get(topic).getPartition(0);
        TopicName secondPartition = TopicName.get(topic).getPartition(1);
        admin.topics().createPartitionedTopic(topic, 2);
        admin.topics().createSubscription(firstPartition.toString(), subscription, MessageId.earliest);
        admin.topics().createSubscription(secondPartition.toString(), subscription, MessageId.earliest);

        String ownerUrl = admin.lookups().lookupTopic(firstPartition.toString());
        PulsarService owner = brokers.stream().filter(broker -> broker.getBrokerServiceUrl().equals(ownerUrl))
                .findFirst().orElseThrow();
        int coordinator = IntStream.range(0, brokers.size())
                .filter(index -> brokers.get(index) != owner)
                .findFirst().orElseThrow();
        String adminUrl = getWebServiceUrl(coordinator) + "/admin/v2/";
        URI uri = URI.create(adminUrl + firstPartition.getRestPath() + "/subscription/"
                + subscription + "/internalStats");
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build();
        try (HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()) {
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            assertThat(response.statusCode()).isEqualTo(307);
            URI location = URI.create(response.headers().firstValue("Location").orElseThrow());
            assertThat(location.getAuthority()).isEqualTo(URI.create(owner.getWebServiceAddress()).getAuthority());

            URI partitionedUri = URI.create(adminUrl + TopicName.get(topic).getRestPath() + "/subscription/"
                    + subscription + "/partitioned-internalStats");
            HttpRequest partitionedRequest = HttpRequest.newBuilder(partitionedUri)
                    .timeout(Duration.ofSeconds(10)).GET().build();
            HttpResponse<String> partitionedResponse = client.send(partitionedRequest,
                    HttpResponse.BodyHandlers.ofString());
            assertThat(partitionedResponse.statusCode()).as("response body: %s", partitionedResponse.body())
                    .isEqualTo(200);
            PartitionedSubscriptionCursorInternalStats result = ObjectMapperFactory.getMapper().reader()
                    .forType(PartitionedSubscriptionCursorInternalStats.class).readValue(partitionedResponse.body());
            assertThat(result.complete).isTrue();
            assertThat(result.partitions).containsOnlyKeys(firstPartition.toString(), secondPartition.toString());
            assertThat(result.errors).isEmpty();
        }
    }
}
