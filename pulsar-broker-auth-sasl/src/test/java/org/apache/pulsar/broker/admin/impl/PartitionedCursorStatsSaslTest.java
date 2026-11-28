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
package org.apache.pulsar.broker.admin.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import jakarta.ws.rs.client.ClientBuilder;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.impl.auth.AuthenticationSasl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.testng.annotations.Test;

public class PartitionedCursorStatsSaslTest {
    @Test
    public void testSaslHandshakeTimeoutInPartialResult() throws Exception {
        var queuedCredentials = new LinkedBlockingQueue<Runnable>();
        var services = mock(ClientAuthenticationServices.class);
        when(services.blockingExecutor()).thenReturn(queuedCredentials::add);
        var auth = new AuthenticationSasl();
        auth.bindClientAuthenticationServices(services);
        var topic = TopicName.get("persistent://public/default/cursor-stats");
        try (var client = ClientBuilder.newClient(); var executor = Executors.newSingleThreadScheduledExecutor()) {
            var topics = new TopicsImpl(client.target("http://localhost"), auth, 1000);
            var response = PartitionedSubscriptionCursorStats.collect(topic, new PartitionedTopicMetadata(2), false,
                    deadline -> CompletableFuture.completedFuture((partition, result) -> {
                        if (partition.equals(topic.getPartition(0).toString())) {
                            result.complete(new CursorStats());
                        } else {
                            topics.getLocalSubscriptionCursorInternalStatsAsync(partition, "sub", false, result);
                        }
                    }), executor, Duration.ofSeconds(30)).get(10, TimeUnit.SECONDS);

            assertThat(queuedCredentials).hasSize(1);
            assertThat(response.complete).isFalse();
            assertThat(response.partitions).containsOnlyKeys(topic.getPartition(0).toString());
            var error = response.errors.get(topic.getPartition(1).toString());
            assertThat(error.statusCode).isEqualTo(503);
            assertThat(error.errorCode).isEqualTo("TIMEOUT");
        }
    }
}
