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
package org.apache.pulsar.functions.instance.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.v5.async.AsyncMessageBuilder;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.testng.annotations.Test;

public class V5ProducerAdapterTest {

    @SuppressWarnings("unchecked")
    private static AsyncMessageBuilder<String> mockMessageBuilder() {
        AsyncMessageBuilder<String> builder = mock(AsyncMessageBuilder.class);
        when(builder.value(any())).thenReturn(builder);
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static V5ProducerAdapter<String> adapter(AsyncMessageBuilder<String> builder) {
        org.apache.pulsar.client.api.v5.Producer<String> producer =
                mock(org.apache.pulsar.client.api.v5.Producer.class);
        AsyncProducer<String> asyncProducer = mock(AsyncProducer.class);
        when(producer.async()).thenReturn(asyncProducer);
        when(producer.topic()).thenReturn("topic://public/default/out");
        when(asyncProducer.newMessage()).thenReturn(builder);
        when(asyncProducer.flush()).thenReturn(CompletableFuture.completedFuture(null));
        return new V5ProducerAdapter<>(producer, Schema.STRING);
    }

    @Test
    public void testMessageFieldsMapToV5Builder() throws Exception {
        AsyncMessageBuilder<String> builder = mockMessageBuilder();
        org.apache.pulsar.client.api.v5.MessageId v5MessageId = mock(org.apache.pulsar.client.api.v5.MessageId.class);
        when(v5MessageId.toByteArray()).thenReturn(new byte[]{1, 2});
        when(builder.send()).thenReturn(CompletableFuture.completedFuture(v5MessageId));

        MessageId messageId = adapter(builder).newMessage()
                .key("k")
                .value("v")
                .property("a", "1")
                .properties(Map.of("b", "2"))
                .eventTime(1000L)
                .sequenceId(7L)
                .replicationClusters(List.of("c1"))
                .deliverAt(2000L)
                .deliverAfter(3, TimeUnit.SECONDS)
                .send();

        verify(builder).key("k");
        verify(builder).value("v");
        verify(builder).property("a", "1");
        verify(builder).properties(Map.of("b", "2"));
        verify(builder).eventTime(Instant.ofEpochMilli(1000L));
        verify(builder).sequenceId(7L);
        verify(builder).replicationClusters(List.of("c1"));
        verify(builder).deliverAt(Instant.ofEpochMilli(2000L));
        verify(builder).deliverAfter(Duration.ofSeconds(3));
        assertThat(messageId).isInstanceOf(V5MessageIdAdapter.class);
        assertThat(((V5MessageIdAdapter) messageId).v5MessageId()).isSameAs(v5MessageId);
        assertThat(messageId.toByteArray()).containsExactly(1, 2);
    }

    @Test
    public void testDisableReplicationAndLoadConf() {
        AsyncMessageBuilder<String> builder = mockMessageBuilder();
        adapter(builder).newMessage().loadConf(Map.of(
                TypedMessageBuilder.CONF_KEY, "k",
                TypedMessageBuilder.CONF_DISABLE_REPLICATION, true,
                TypedMessageBuilder.CONF_DELIVERY_AFTER_SECONDS, 5L));
        verify(builder).key("k");
        verify(builder).replicationClusters(List.of("__local__"));
        verify(builder).deliverAfter(Duration.ofSeconds(5));

        assertThatThrownBy(() -> adapter(builder).newMessage().loadConf(Map.of("unknown", 1)))
                .hasMessageContaining("Invalid message config key 'unknown'");
    }

    @Test
    public void testUnsupportedBuilderFeatures() {
        TypedMessageBuilder<String> message = adapter(mockMessageBuilder()).newMessage();
        assertThatThrownBy(() -> message.keyBytes(new byte[]{1}))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> message.orderingKey(new byte[]{1}))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testNewMessageAcceptsOnlyTheProducerSchema() {
        V5ProducerAdapter<String> producer = adapter(mockMessageBuilder());
        assertThat(producer.newMessage(Schema.STRING)).isNotNull();
        assertThat(producer.newMessage((Schema<String>) null)).isNotNull();
        assertThatThrownBy(() -> producer.newMessage(Schema.BYTES))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("per-message schemas");
    }

    @Test
    public void testProducerCacheContract() throws Exception {
        V5ProducerAdapter<String> producer = adapter(mockMessageBuilder());
        // the producer cache weighs producers by partition count and flushes them before closing
        assertThat(producer.getNumOfPartitions()).isZero();
        assertThat(producer.flushAsync()).isCompleted();
        assertThat(producer.getTopic()).isEqualTo("topic://public/default/out");
    }
}
