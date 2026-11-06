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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.testng.annotations.Test;

/** New observability must not change the internal classic lookups that feed existing broker metrics. */
@Test(groups = "broker")
public class DagWatchSessionLookupCompatibilityTest {
    private static final TopicName TOPIC = TopicName.get("topic://tenant/ns/topic");
    private static final String PATH = "/topics/tenant/ns/topic";
    private static final HashRange LEFT = HashRange.of(0, 0x7fff);
    private static final HashRange RIGHT = HashRange.of(0x8000, 0xffff);
    private static final ScalableTopicMetadata METADATA = ScalableTopicMetadata.builder()
            .epoch(1).nextSegmentId(3).segments(Map.of(
                    0L, SegmentInfo.active(0, HashRange.of(0, 0xffff), 0, 1).sealed(1, 2, List.of(1L, 2L)),
                    1L, SegmentInfo.active(1, LEFT, List.of(0L), 1, 2),
                    2L, SegmentInfo.active(2, RIGHT, List.of(0L), 1, 2))).build();

    @Test
    public void collectionAndStoppingMetricsDoNotAddOrSuppressClassicLookups() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.session.pushInitialResponse(fixture.session.start().get(10, TimeUnit.SECONDS));
            fixture.writes.remove().setSuccess();
            fixture.assertBuilds(1);
            fixture.collect();
            fixture.session.stopMetricsTracking();
            fixture.collect();
            fixture.assertBuilds(1);
            verify(fixture.resources, never()).deregisterPathListener(any());

            fixture.notifyChange();
            fixture.writes.remove().setSuccess();
            fixture.collect();
            fixture.assertBuilds(2);
            verify(fixture.ctx, times(2)).writeAndFlush(any());
            verify(fixture.resources, times(2)).getScalableTopicMetadataAsync(TOPIC, true);
        }
    }

    @Test
    public void failedInitialWriteDoesNotStopOriginalNotificationWorkload() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.session.pushInitialResponse(fixture.session.start().get(10, TimeUnit.SECONDS));
            fixture.writes.remove().setFailure(new IllegalStateException("write failed"));
            fixture.assertBuilds(1);
            verify(fixture.resources, never()).deregisterPathListener(any());

            fixture.notifyChange();
            fixture.writes.remove().setSuccess();
            fixture.assertBuilds(2);
            verify(fixture.ctx, times(2)).writeAndFlush(any());
            verify(fixture.resources, never()).deregisterPathListener(any());
        }
    }

    @Test
    public void asynchronousLookupFailureDoesNotRetryOrSuppressLaterUpdates() {
        try (Fixture fixture = new Fixture()) {
            when(fixture.namespace.getBrokerServiceUrlAsync(eq(SegmentTopicName.fromParent(TOPIC, LEFT, 1)), any()))
                    .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("lookup failed")));
            fixture.notifyChange();
            fixture.assertBuilds(1);
            assertThat(fixture.writes).isEmpty();
            fixture.collect();
            fixture.assertBuilds(1);
            verify(fixture.resources, never()).deregisterPathListener(any());

            when(fixture.namespace.getBrokerServiceUrlAsync(eq(SegmentTopicName.fromParent(TOPIC, LEFT, 1)), any()))
                    .thenReturn(CompletableFuture.completedFuture(Optional.of(fixture.lookup)));
            fixture.notifyChange();
            fixture.writes.remove().setSuccess();
            fixture.assertBuilds(2);
            verify(fixture.ctx).writeAndFlush(any());
            verify(fixture.resources, never()).deregisterPathListener(any());
        }
    }

    @Test
    public void synchronousBuildFailureStillEscapesWithoutAdditionalWork() {
        try (Fixture fixture = new Fixture()) {
            var failure = new IllegalStateException("synchronous lookup failure");
            when(fixture.namespace.getBrokerServiceUrlAsync(any(), any())).thenThrow(failure);
            assertThatThrownBy(() -> fixture.session.onMetadataChanged(METADATA)).isSameAs(failure);
            verify(fixture.namespace).getBrokerServiceUrlAsync(any(), any());
            verify(fixture.store, never()).get(any());
            verify(fixture.ctx, never()).writeAndFlush(any());
            verify(fixture.resources, never()).deregisterPathListener(any());
            fixture.collect();
            verify(fixture.namespace).getBrokerServiceUrlAsync(any(), any());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final NamespaceService namespace = mock(NamespaceService.class);
        private final ScalableTopicResources resources = mock(ScalableTopicResources.class);
        private final MetadataStore store = mock(MetadataStore.class);
        private final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        private final Queue<ChannelPromise> writes = new ArrayDeque<>();
        private final CollectorRegistry registry = new CollectorRegistry();
        private final InMemoryMetricReader reader = InMemoryMetricReader.create();
        private final SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        private final ScalableTopicMetrics metrics = new ScalableTopicMetrics(provider.get("test"), "test", registry);
        private final LookupResult lookup = LookupResult.builder().type(LookupResult.Type.BrokerUrl)
                .brokerServiceUrl("pulsar://broker:6650").build();
        private final DagWatchSession session;

        private Fixture() {
            PulsarService pulsar = mock(PulsarService.class);
            BrokerService broker = mock(BrokerService.class);
            ServerCnx cnx = mock(ServerCnx.class);
            when(broker.getPulsar()).thenReturn(pulsar);
            when(pulsar.getNamespaceService()).thenReturn(namespace);
            when(broker.getScalableTopicMetrics()).thenReturn(metrics);
            when(cnx.ctx()).thenReturn(ctx);
            when(resources.topicPath(TOPIC)).thenReturn(PATH);
            when(resources.controllerLockPath(TOPIC)).thenReturn(PATH + "/controller");
            when(resources.getStore()).thenReturn(store);
            when(resources.getScalableTopicMetadataAsync(TOPIC, true))
                    .thenReturn(CompletableFuture.completedFuture(Optional.of(METADATA)));
            when(store.get(any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
            when(namespace.getBrokerServiceUrlAsync(any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(Optional.of(lookup)));
            when(ctx.writeAndFlush(any())).thenAnswer(invocation -> {
                ByteBuf command = invocation.getArgument(0);
                command.release();
                ChannelPromise write = new DefaultChannelPromise(mock(Channel.class), ImmediateEventExecutor.INSTANCE);
                writes.add(write);
                return write;
            });
            session = new DagWatchSession(1, TOPIC, cnx, resources, broker, false,
                    metrics.startRequest(ScalableTopicMetrics.RequestType.LOOKUP));
        }

        private void notifyChange() {
            session.onNotification(new Notification(NotificationType.Modified, PATH));
        }

        private void collect() {
            assertThat(Collections.list(registry.metricFamilySamples())).isNotEmpty();
            assertThat(reader.collectAllMetrics()).isNotEmpty();
        }

        private void assertBuilds(int count) {
            // This is the existing NamespaceService entrypoint that measures classic lookup metrics.
            verify(namespace, times(count * 2)).getBrokerServiceUrlAsync(any(), any());
            verify(namespace, times(count)).getBrokerServiceUrlAsync(eq(SegmentTopicName.fromParent(TOPIC, LEFT, 1)),
                    any());
            verify(namespace, times(count)).getBrokerServiceUrlAsync(eq(SegmentTopicName.fromParent(TOPIC, RIGHT, 2)),
                    any());
            verify(namespace, never()).getBrokerServiceUrlAsync(
                    eq(SegmentTopicName.fromParent(TOPIC, HashRange.of(0, 0xffff), 0)), any());
            verify(store, times(count)).get(PATH + "/controller");
        }

        @Override
        public void close() {
            session.close();
            writes.forEach(write -> write.tryFailure(new IllegalStateException("test finished")));
            metrics.close();
            provider.close();
        }
    }
}
