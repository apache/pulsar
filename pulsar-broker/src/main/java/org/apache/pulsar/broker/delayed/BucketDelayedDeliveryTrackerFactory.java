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
package org.apache.pulsar.broker.delayed;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.collections4.MapUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.delayed.bucket.BookkeeperBucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.bucket.BucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.bucket.RecoverDelayedDeliveryTrackerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.util.FutureUtil;

@CustomLog
public class BucketDelayedDeliveryTrackerFactory implements DelayedDeliveryTrackerFactory {
    BucketSnapshotStorage bucketSnapshotStorage;

    private Timer timer;

    private long tickTimeMillis;

    private boolean isDelayedDeliveryDeliverAtTimeStrict;

    private int delayedDeliveryMaxNumBuckets;

    private long delayedDeliveryMinIndexCountPerBucket;

    private int delayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds;

    private int delayedDeliveryMaxIndexesPerBucketSnapshotSegment;

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        ServiceConfiguration config = pulsarService.getConfig();
        bucketSnapshotStorage = new BookkeeperBucketSnapshotStorage(pulsarService);
        bucketSnapshotStorage.start();
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-delayed-delivery"),
                config.getDelayedDeliveryTickTimeMillis(), TimeUnit.MILLISECONDS);
        this.tickTimeMillis = config.getDelayedDeliveryTickTimeMillis();
        this.isDelayedDeliveryDeliverAtTimeStrict = config.isDelayedDeliveryDeliverAtTimeStrict();
        this.delayedDeliveryMinIndexCountPerBucket = config.getDelayedDeliveryMinIndexCountPerBucket();
        this.delayedDeliveryMaxNumBuckets = config.getDelayedDeliveryMaxNumBuckets();
        this.delayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds =
                config.getDelayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds();
        this.delayedDeliveryMaxIndexesPerBucketSnapshotSegment =
                config.getDelayedDeliveryMaxIndexesPerBucketSnapshotSegment();
    }

    @Override
    public DelayedDeliveryTracker newTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String topicName = dispatcher.getTopic().getName();
        String subscriptionName = dispatcher.getSubscription().getName();
        DelayedDeliveryTracker tracker;

        try {
            tracker = newTracker0(dispatcher);
            ((BucketDelayedDeliveryTracker) tracker).recoverBucketSnapshot();
        } catch (RecoverDelayedDeliveryTrackerException ex) {
            tracker = newFallbackTracker(dispatcher, topicName, subscriptionName, ex);
        }
        return tracker;
    }

    @Override
    public CompletableFuture<DelayedDeliveryTracker> newTrackerAsync(
            AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String topicName = null;
        String subscriptionName = null;
        try {
            topicName = dispatcher.getTopic().getName();
            subscriptionName = dispatcher.getSubscription().getName();
            String currentTopicName = topicName;
            String currentSubscriptionName = subscriptionName;
            BucketDelayedDeliveryTracker tracker;
            tracker = newTracker0(dispatcher);
            return tracker.recoverBucketSnapshotAsync(dispatcher.getTopic().getBrokerService().executor())
                    .thenApply(delayedDeliveryTracker -> (DelayedDeliveryTracker) delayedDeliveryTracker)
                    .exceptionallyCompose(ex -> newFallbackTrackerAsync(dispatcher, currentTopicName,
                            currentSubscriptionName, FutureUtil.unwrapCompletionException(ex)));
        } catch (RecoverDelayedDeliveryTrackerException ex) {
            return newFallbackTrackerAsync(dispatcher, topicName, subscriptionName, ex);
        } catch (Throwable t) {
            return FutureUtil.failedFuture(t);
        }
    }

    private DelayedDeliveryTracker newFallbackTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                                      String topicName,
                                                      String subscriptionName,
                                                      Throwable ex) {
        BrokerService brokerService = dispatcher.getTopic().getBrokerService();
        brokerService.initializeFallbackDelayedDeliveryTrackerFactory();
        DelayedDeliveryTrackerFactory fallbackFactory = brokerService.getFallbackDelayedDeliveryTrackerFactory();
        log.warn()
                .attr("topic", topicName)
                .attr("subscription", subscriptionName)
                .attr("dispatcher", dispatcher.getName())
                .attr("async", false)
                .attr("fallbackTrackerFactory", delayedDeliveryTrackerFactoryType(fallbackFactory))
                .exception(ex)
                .log("Failed to recover BucketDelayedDeliveryTracker, fallback to InMemoryDelayedDeliveryTracker");
        return fallbackFactory.newTracker(dispatcher);
    }

    private CompletableFuture<DelayedDeliveryTracker> newFallbackTrackerAsync(
            AbstractPersistentDispatcherMultipleConsumers dispatcher, String topicName, String subscriptionName,
            Throwable ex) {
        try {
            BrokerService brokerService = dispatcher.getTopic().getBrokerService();
            brokerService.initializeFallbackDelayedDeliveryTrackerFactory();
            DelayedDeliveryTrackerFactory fallbackFactory = brokerService.getFallbackDelayedDeliveryTrackerFactory();
            log.warn()
                    .attr("topic", topicName)
                    .attr("subscription", subscriptionName)
                    .attr("dispatcher", dispatcher.getName())
                    .attr("async", true)
                    .attr("fallbackTrackerFactory", delayedDeliveryTrackerFactoryType(fallbackFactory))
                    .exception(ex)
                    .log("Failed to recover BucketDelayedDeliveryTracker, fallback to InMemoryDelayedDeliveryTracker");
            return fallbackFactory.newTrackerAsync(dispatcher).whenComplete((__, fallbackEx) -> {
                if (fallbackEx != null) {
                    log.error()
                            .attr("topic", topicName)
                            .attr("subscription", subscriptionName)
                            .attr("dispatcher", dispatcher.getName())
                            .attr("fallbackTrackerFactory", delayedDeliveryTrackerFactoryType(fallbackFactory))
                            .exception(FutureUtil.unwrapCompletionException(fallbackEx))
                            .log("Failed to create fallback delayed delivery tracker");
                }
            });
        } catch (Throwable t) {
            log.error()
                    .attr("topic", topicName)
                    .attr("subscription", subscriptionName)
                    .attr("dispatcher", dispatcher.getName())
                    .attr("recoveryExceptionType", ex == null ? "null" : ex.getClass().getName())
                    .exception(t)
                    .log("Failed to create fallback delayed delivery tracker");
            return FutureUtil.failedFuture(t);
        }
    }

    private String delayedDeliveryTrackerFactoryType(DelayedDeliveryTrackerFactory factory) {
        return factory == null ? "null" : factory.getClass().getSimpleName();
    }

    @VisibleForTesting
    BucketDelayedDeliveryTracker newTracker0(AbstractPersistentDispatcherMultipleConsumers dispatcher)
            throws RecoverDelayedDeliveryTrackerException {
        return new BucketDelayedDeliveryTracker(new DispatcherDelayedDeliveryContext(dispatcher), timer,
                tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                bucketSnapshotStorage, delayedDeliveryMinIndexCountPerBucket,
                TimeUnit.SECONDS.toMillis(delayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds),
                delayedDeliveryMaxIndexesPerBucketSnapshotSegment, delayedDeliveryMaxNumBuckets, false);
    }

    /**
     * Clean up residual snapshot data.
     * If tracker has not been created or has been closed, then we can't clean up the snapshot with `tracker.clear`,
     * this method can clean up the residual snapshots without creating a tracker.
     */
    public CompletableFuture<Void> cleanResidualSnapshots(ManagedCursor cursor) {
        Map<String, String> cursorProperties = cursor.getCursorProperties();
        if (MapUtils.isEmpty(cursorProperties)) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        FutureUtil.Sequencer<Void> sequencer = FutureUtil.Sequencer.create();
        cursorProperties.forEach((k, v) -> {
            if (k != null && v != null && k.startsWith(BucketDelayedDeliveryTracker.DELAYED_BUCKET_KEY_PREFIX)) {
                CompletableFuture<Void> future = sequencer.sequential(() ->
                        bucketSnapshotStorage.deleteBucketSnapshot(Long.parseLong(v))
                                .thenCompose(__ -> cursor.removeCursorProperty(k)));
                futures.add(future);
            }
        });

        return FutureUtil.waitForAll(futures);
    }

    @Override
    public void close() throws Exception {
        if (bucketSnapshotStorage != null) {
            bucketSnapshotStorage.close();
        }
        if (timer != null) {
            timer.stop();
        }
    }
}
