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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ScalableConsumerSession;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.proto.ScalableAssignedSegment;
import org.apache.pulsar.common.api.proto.ScalableConsumerAssignment;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.scalable.HashRange;

/**
 * Client-side session for a scalable-topic consumer (Stream or Checkpoint).
 *
 * <p>Sends a {@code CommandScalableTopicSubscribe} on attach, awaits the initial
 * {@link ScalableConsumerAssignment}, and then accepts pushed updates via
 * {@link #onAssignmentUpdate}. The current assignment is exposed as a list of
 * {@link ActiveSegment} entries (segmentId, hashRange, segmentTopic) so the consumer
 * implementations can use the same segment-attach plumbing they already have.
 *
 * <p>Mirrors {@link DagWatchClient} in shape; the wire path is different — subscribe
 * is request/response (matched by request id) and updates are tagged with the
 * client-chosen consumer id.
 */
final class ScalableConsumerClient implements ScalableConsumerSession, AutoCloseable {

    private static final Logger LOG = Logger.get(ScalableConsumerClient.class);
    private final Logger log;

    private final PulsarClientImpl v4Client;
    private final TopicName topicName;
    private final String subscription;
    private final String consumerName;
    private final long consumerId;
    private final ScalableConsumerType consumerType;

    private final AtomicReference<List<ActiveSegment>> currentAssignment =
            new AtomicReference<>(List.of());
    private volatile long currentEpoch = -1L;
    private final CompletableFuture<List<ActiveSegment>> initialAssignmentFuture =
            new CompletableFuture<>();
    private volatile AssignmentChangeListener listener;
    private volatile ClientCnx cnx;
    private volatile boolean closed = false;

    ScalableConsumerClient(PulsarClientImpl v4Client,
                           TopicName topicName,
                           String subscription,
                           String consumerName,
                           ScalableConsumerType consumerType) {
        this.v4Client = v4Client;
        this.topicName = topicName;
        this.subscription = subscription;
        this.consumerName = consumerName;
        this.consumerId = v4Client.newConsumerId();
        this.consumerType = consumerType;
        this.log = LOG.with()
                .attr("topic", topicName)
                .attr("subscription", subscription)
                .attr("consumerName", consumerName)
                .attr("consumerId", consumerId)
                .build();
    }

    /**
     * Resolve the controller-leader broker URL via a DAG-watch lookup, open a
     * connection to it directly (no v4 topic lookup — scalable topic URIs aren't
     * resolvable through the v4 lookup service), then send the subscribe command and
     * complete with the initial assignment.
     */
    CompletableFuture<List<ActiveSegment>> start() {
        DagWatchClient watch = new DagWatchClient(v4Client, topicName);
        watch.start()
                .thenCompose(layout -> {
                    String controllerUrl = layout.controllerBrokerUrl();
                    if (controllerUrl == null || controllerUrl.isEmpty()) {
                        // Controller leader election hasn't completed yet (or the broker
                        // doesn't advertise the URL). Fall back to the configured
                        // service URL — any broker will forward the subscribe request
                        // to the controller via getOrCreateController.
                        log.info()
                                .log("Layout has no controller URL; connecting to service URL");
                        return v4Client.getConnectionToServiceUrl();
                    }
                    URI uri = URI.create(controllerUrl);
                    InetSocketAddress addr = InetSocketAddress.createUnresolved(
                            uri.getHost(), uri.getPort());
                    return v4Client.getConnection(addr, addr,
                            v4Client.getCnxPool().genRandomKeyToSelectCon());
                })
                .whenComplete((cnx, ex) -> {
                    // Close the watch session as soon as we have the controller URL —
                    // the controller pushes assignment updates directly, so we don't
                    // need a long-lived layout watch on this consumer.
                    watch.close();
                })
                .thenAccept(cnx -> {
                    this.cnx = cnx;
                    if (!cnx.isSupportsScalableTopics()) {
                        initialAssignmentFuture.completeExceptionally(
                                new PulsarClientException.FeatureNotSupportedException(
                                        "Broker does not support scalable topics",
                                        PulsarClientException.FailedFeatureCheck.SupportsScalableTopics));
                        return;
                    }
                    cnx.registerScalableConsumerSession(consumerId, this);

                    long requestId = v4Client.newRequestId();
                    var responseFuture = new TimedCompletableFuture<ScalableConsumerAssignment>();
                    cnx.getPendingRequests().put(requestId, responseFuture);

                    cnx.ctx().writeAndFlush(Commands.newScalableTopicSubscribe(
                                    requestId,
                                    topicName.toString(),
                                    subscription,
                                    consumerName,
                                    consumerId,
                                    consumerType))
                            .addListener(writeFuture -> {
                                if (!writeFuture.isSuccess()) {
                                    cnx.getPendingRequests().remove(requestId);
                                    cnx.removeScalableConsumerSession(consumerId);
                                    initialAssignmentFuture.completeExceptionally(
                                            new PulsarClientException(writeFuture.cause()));
                                }
                            });

                    responseFuture.whenComplete((assignment, ex) -> {
                        if (ex != null) {
                            initialAssignmentFuture.completeExceptionally(ex);
                            return;
                        }
                        List<ActiveSegment> segments = toSegmentList(assignment);
                        currentAssignment.set(segments);
                        currentEpoch = assignment.getLayoutEpoch();
                        log.info().attr("epoch", currentEpoch)
                                .attr("segments", segments.size())
                                .log("Initial assignment received");
                        initialAssignmentFuture.complete(segments);
                    });
                })
                .exceptionally(ex -> {
                    initialAssignmentFuture.completeExceptionally(ex);
                    return null;
                });
        return initialAssignmentFuture;
    }

    @Override
    public void onAssignmentUpdate(ScalableConsumerAssignment assignment) {
        if (closed) {
            return;
        }
        long epoch = assignment.getLayoutEpoch();
        if (epoch < currentEpoch) {
            log.info().attr("staleEpoch", epoch).attr("currentEpoch", currentEpoch)
                    .log("Ignoring stale assignment update");
            return;
        }
        List<ActiveSegment> newSegments = toSegmentList(assignment);
        List<ActiveSegment> oldSegments = currentAssignment.getAndSet(newSegments);
        currentEpoch = epoch;
        log.info().attr("epoch", epoch).attr("segments", newSegments.size())
                .log("Assignment updated");

        AssignmentChangeListener l = listener;
        if (l != null) {
            try {
                l.onAssignmentChange(newSegments, oldSegments);
            } catch (Exception e) {
                log.error().exception(e).log("Error in assignment change listener");
            }
        }
    }

    @Override
    public void connectionClosed() {
        log.warn("Scalable consumer session connection closed");
        cnx = null;
        if (!initialAssignmentFuture.isDone()) {
            initialAssignmentFuture.completeExceptionally(
                    new PulsarClientException("Connection closed before initial assignment arrived"));
        }
        // TODO: implement automatic re-subscribe on reconnect
    }

    private static List<ActiveSegment> toSegmentList(ScalableConsumerAssignment assignment) {
        List<ActiveSegment> segments = new ArrayList<>(assignment.getSegmentsCount());
        for (int i = 0; i < assignment.getSegmentsCount(); i++) {
            ScalableAssignedSegment s = assignment.getSegmentAt(i);
            segments.add(new ActiveSegment(
                    s.getSegmentId(),
                    HashRange.of((int) s.getHashStart(), (int) s.getHashEnd()),
                    s.getSegmentTopic()));
        }
        return Collections.unmodifiableList(segments);
    }

    List<ActiveSegment> currentAssignment() {
        return currentAssignment.get();
    }

    void setListener(AssignmentChangeListener listener) {
        this.listener = listener;
    }

    long consumerId() {
        return consumerId;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        ClientCnx c = cnx;
        if (c != null) {
            c.removeScalableConsumerSession(consumerId);
            // No close command for now — broker reaps registrations via grace timer on
            // disconnect. A future refactor can add an explicit unsubscribe.
        }
    }

    interface AssignmentChangeListener {
        void onAssignmentChange(List<ActiveSegment> newSegments, List<ActiveSegment> oldSegments);
    }
}
