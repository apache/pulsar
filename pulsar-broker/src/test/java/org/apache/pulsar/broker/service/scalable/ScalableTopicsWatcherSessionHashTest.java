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

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Coverage for the reconnect-hash short-circuit in
 * {@link ScalableTopicsWatcherSession}: when the client's reported hash matches
 * the freshly-computed server hash, {@code start()} returns without writing a
 * Snapshot frame; otherwise it emits Snapshot as usual.
 */
public class ScalableTopicsWatcherSessionHashTest {

    private MetadataStoreExtended store;
    private ScalableTopicResources resources;
    private ServerCnx cnx;
    private ChannelHandlerContext ctx;
    private ScheduledExecutorService scheduler;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().build());
        resources = new ScalableTopicResources(store, 30);
        cnx = mock(ServerCnx.class);
        ctx = mock(ChannelHandlerContext.class);
        when(cnx.ctx()).thenReturn(ctx);
        // writeAndFlush returns a ChannelFuture; provide a no-op promise to keep the
        // call chain happy. The ScalableTopicsWatcherSession ignores the return.
        when(ctx.writeAndFlush(org.mockito.ArgumentMatchers.any()))
                .thenReturn(new DefaultChannelPromise(new EmbeddedChannel(),
                        ImmediateEventExecutor.INSTANCE));
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (store != null) {
            store.close();
        }
    }

    private ScalableTopicMetadata meta() {
        return ScalableTopicMetadata.builder().epoch(0).nextSegmentId(1)
                .properties(Map.of()).build();
    }

    @Test
    public void noHashOnFirstSubscribeEmitsSnapshot() throws Exception {
        NamespaceName ns = NamespaceName.get("tenant/ns-fresh-" + suffix());
        TopicName tn = TopicName.get("topic://" + ns + "/t1");
        resources.createScalableTopicAsync(tn, meta()).get();

        var session = new ScalableTopicsWatcherSession(1L, ns, Map.of(),
                /* clientHash= */ null, cnx, resources, scheduler);
        session.start().get();

        // First subscribe: no hash → broker must emit one Snapshot frame.
        verify(ctx, atLeastOnce()).writeAndFlush(org.mockito.ArgumentMatchers.any(ByteBuf.class));
    }

    @Test
    public void matchingHashOnReconnectSkipsSnapshot() throws Exception {
        NamespaceName ns = NamespaceName.get("tenant/ns-match-" + suffix());
        TopicName tn = TopicName.get("topic://" + ns + "/t1");
        resources.createScalableTopicAsync(tn, meta()).get();

        // Client believes the namespace contains exactly this one topic. Compute the
        // matching hash with the same function the broker uses (TopicList.crc32c).
        String matchingHash = TopicList.calculateHash(List.of(tn.toString()));

        var session = new ScalableTopicsWatcherSession(2L, ns, Map.of(),
                /* clientHash= */ matchingHash, cnx, resources, scheduler);
        session.start().get();

        // Hash matched → no Snapshot frame written. Future Diffs would still flow
        // through writeAndFlush, but start() itself must stay silent.
        verify(ctx, never()).writeAndFlush(org.mockito.ArgumentMatchers.any(ByteBuf.class));
    }

    @Test
    public void differingHashOnReconnectStillEmitsSnapshot() throws Exception {
        NamespaceName ns = NamespaceName.get("tenant/ns-diff-" + suffix());
        TopicName tn = TopicName.get("topic://" + ns + "/t1");
        resources.createScalableTopicAsync(tn, meta()).get();

        // Client thinks the set is something different. Broker must emit a fresh
        // Snapshot so the client can reconcile.
        String staleHash = TopicList.calculateHash(List.of("topic://" + ns + "/something-else"));

        var session = new ScalableTopicsWatcherSession(3L, ns, Map.of(),
                /* clientHash= */ staleHash, cnx, resources, scheduler);
        session.start().get();

        verify(ctx, atLeastOnce()).writeAndFlush(org.mockito.ArgumentMatchers.any(ByteBuf.class));
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
