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
package org.apache.pulsar.client.impl.v5;

import io.github.merlimat.slog.Logger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.DagWatchSession;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Client-side manager for a DAG watch session on a scalable topic.
 *
 * <p>Connects to any broker, sends a ScalableTopicLookup to initiate a watch session,
 * and receives pushed updates when the segment layout changes (split/merge).
 *
 * <p>Maintains the current {@link ClientSegmentLayout} and notifies a listener
 * when it changes.
 */
final class DagWatchClient implements DagWatchSession, AutoCloseable {

    private static final Logger LOG = Logger.get(DagWatchClient.class);
    private final Logger log;

    private static final AtomicLong SESSION_ID_GENERATOR = new AtomicLong(0);

    private final PulsarClientImpl v4Client;
    private final TopicName topicName;
    private final long sessionId;
    private final AtomicReference<ClientSegmentLayout> currentLayout = new AtomicReference<>();
    private final CompletableFuture<ClientSegmentLayout> initialLayoutFuture = new CompletableFuture<>();
    private volatile LayoutChangeListener listener;
    private volatile ClientCnx cnx;
    private volatile boolean closed = false;

    DagWatchClient(PulsarClientImpl v4Client, TopicName topicName) {
        this.v4Client = v4Client;
        this.topicName = topicName;
        this.sessionId = SESSION_ID_GENERATOR.incrementAndGet();
        this.log = LOG.with().attr("topic", topicName).attr("sessionId", sessionId).build();
    }

    /**
     * Start the DAG watch session. Connects to any broker and sends a
     * ScalableTopicLookup to initiate the watch session.
     *
     * @return a future that completes with the initial layout
     */
    CompletableFuture<ClientSegmentLayout> start() {
        // Get any broker connection and send the lookup command
        v4Client.getConnection(topicName.toString())
                .thenAccept(cnx -> {
                    this.cnx = cnx;
                    // Register this session to receive updates
                    cnx.registerDagWatchSession(sessionId, this);

                    // Send the lookup command
                    cnx.ctx().writeAndFlush(
                            Commands.newScalableTopicLookup(sessionId, topicName.toString()))
                            .addListener(writeFuture -> {
                                if (!writeFuture.isSuccess()) {
                                    cnx.removeDagWatchSession(sessionId);
                                    initialLayoutFuture.completeExceptionally(
                                            new PulsarClientException(writeFuture.cause()));
                                }
                            });
                })
                .exceptionally(ex -> {
                    initialLayoutFuture.completeExceptionally(ex);
                    return null;
                });

        return initialLayoutFuture;
    }

    /**
     * Called when the broker pushes a ScalableTopicUpdate for this session.
     * This is invoked from the Netty I/O thread.
     */
    @Override
    public void onUpdate(ScalableTopicDAG dag) {
        if (closed) {
            return;
        }

        ClientSegmentLayout newLayout = ClientSegmentLayout.fromProto(dag, topicName);
        ClientSegmentLayout oldLayout = currentLayout.getAndSet(newLayout);

        log.info().attr("oldEpoch", oldLayout != null ? oldLayout.epoch() : "none")
                .attr("newEpoch", newLayout.epoch())
                .attr("activeSegmentCount", newLayout.activeSegments().size())
                .log("Layout updated");

        // Complete the initial layout future if this is the first update
        initialLayoutFuture.complete(newLayout);

        LayoutChangeListener l = listener;
        if (l != null) {
            try {
                l.onLayoutChange(newLayout, oldLayout);
            } catch (Exception e) {
                log.error().exception(e).log("Error in layout change listener");
            }
        }
    }

    @Override
    public void onError(ServerError error, String message) {
        log.error().attr("error", error).attr("message", message)
                .log("DAG watch session error");
        initialLayoutFuture.completeExceptionally(
                new PulsarClientException(
                        "Scalable topic lookup failed: " + error + " - " + message));
    }

    @Override
    public void connectionClosed() {
        log.warn("DAG watch session connection closed");
        cnx = null;
        initialLayoutFuture.completeExceptionally(
                new PulsarClientException("Connection closed while waiting for scalable topic layout"));
        // TODO: implement automatic reconnection with backoff
    }

    ClientSegmentLayout currentLayout() {
        return currentLayout.get();
    }

    void setListener(LayoutChangeListener listener) {
        this.listener = listener;
    }

    long sessionId() {
        return sessionId;
    }

    TopicName topicName() {
        return topicName;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        ClientCnx c = cnx;
        if (c != null) {
            c.removeDagWatchSession(sessionId);
            c.ctx().writeAndFlush(Commands.newScalableTopicClose(sessionId));
        }
    }

    interface LayoutChangeListener {
        void onLayoutChange(ClientSegmentLayout newLayout, ClientSegmentLayout oldLayout);
    }
}
