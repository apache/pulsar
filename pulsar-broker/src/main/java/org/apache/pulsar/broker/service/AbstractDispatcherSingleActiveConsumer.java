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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.client.impl.Murmur3Hash32;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;

@CustomLog
public abstract class AbstractDispatcherSingleActiveConsumer extends AbstractBaseDispatcher {

    private static final int MAX_RETRY_COUNT_FOR_ADD_CONSUMER_RACE = 5;
    protected final String topicName;
    private volatile Consumer activeConsumer = null;
    protected final CopyOnWriteArrayList<Consumer> consumers;
    protected StickyKeyConsumerSelector stickyKeyConsumerSelector;
    protected boolean isKeyHashRangeFiltered = false;
    protected CompletableFuture<Void> closeFuture = null;
    protected final int partitionIndex;
    protected final ManagedCursor cursor;
    // This dispatcher supports both the Exclusive and Failover subscription types
    protected final SubType subscriptionType;

    protected static final int FALSE = 0;
    protected static final int TRUE = 1;
    protected static final AtomicIntegerFieldUpdater<AbstractDispatcherSingleActiveConsumer> IS_CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractDispatcherSingleActiveConsumer.class, "isClosed");
    private volatile int isClosed = FALSE;

    protected boolean isFirstRead = true;
    // For compacted reads: the position from which the subscription has been reading the original ledger, after its
    // compacted snapshot. Later compactions must not move it back to the compacted ledger past that point.
    protected Position originalLedgerReadPosition;
    private static final int CONSUMER_CONSISTENT_HASH_REPLICAS = 100;
    // Above this many (partition, consumer) pairs the balanced selection falls back to the hash ring
    private static final long MAX_BALANCED_HASHING_PAIRS = 1_000_000;
    // The partitions of a topic on this broker compute the same balanced assignment, so they share it
    private static final Cache<List<Object>, int[]> BALANCED_ASSIGNMENTS = Caffeine.newBuilder()
            .maximumSize(1_000).expireAfterAccess(1, TimeUnit.MINUTES).build();
    // Partition count of the partitioned topic, remembered because its metadata cache entry can be evicted
    private int numPartitions;

    public AbstractDispatcherSingleActiveConsumer(SubType subscriptionType, int partitionIndex,
                                                  String topicName, Subscription subscription,
                                                  ServiceConfiguration serviceConfig, ManagedCursor cursor) {
        super(subscription, serviceConfig);
        this.topicName = topicName;
        this.consumers = new CopyOnWriteArrayList<>();
        this.partitionIndex = partitionIndex;
        this.subscriptionType = subscriptionType;
        this.cursor = cursor;
    }

    /**
     * @apiNote this method does not need to be thread safe
     */
    protected abstract void scheduleReadOnActiveConsumer();

    /**
     * @apiNote this method does not need to be thread safe
     */
    protected abstract void cancelPendingRead();

    protected void notifyActiveConsumerChanged(Consumer activeConsumer) {
        if (null != activeConsumer && subscriptionType == SubType.Failover) {
            consumers.forEach(consumer ->
                consumer.notifyActiveConsumerChange(activeConsumer));
        }
    }

    /**
     * Pick active consumer for a topic for {@link SubType#Failover} subscription.
     * If it's a non-partitioned topic then it'll pick consumer based on order they subscribe to the topic.
     * If is's a partitioned topic, first sort consumers based on their priority level and consumer name then
     * distributed partitions evenly across consumers with highest priority level.
     *
     * @return the true consumer if the consumer is changed, otherwise false.
     * @apiNote this method is not thread safe
     */
    protected boolean pickAndScheduleActiveConsumer() {
        checkArgument(!consumers.isEmpty());
        AtomicBoolean hasPriorityConsumer = new AtomicBoolean(false);
        consumers.sort((c1, c2) -> {
            int priority = c1.getPriorityLevel() - c2.getPriorityLevel();
            if (priority != 0) {
                hasPriorityConsumer.set(true);
                return priority;
            }
            int name = c1.consumerName().compareTo(c2.consumerName());
            if (name != 0 || partitionIndex < 0) {
                return name;
            }
            // A partition-local consumer id or arrival order cannot identify a logical consumer across partitions.
            return Objects.toString(c1.consumerInstanceId(), "")
                    .compareTo(Objects.toString(c2.consumerInstanceId(), ""));
        });

        int consumersSize = consumers.size();
        // find number of consumers which are having the highest priorities. so partitioned-topic assignment happens
        // evenly across highest priority consumers
        if (hasPriorityConsumer.get()) {
            int highestPriorityLevel = consumers.get(0).getPriorityLevel();
            for (int i = 0; i < consumers.size(); i++) {
                if (highestPriorityLevel != consumers.get(i).getPriorityLevel()) {
                    consumersSize = i;
                    break;
                }
            }
        }
        int index;
        if (partitionIndex < 0) {
            index = peekConsumerIndexFromHashRing(makeHashRing(consumersSize));
        } else if (!serviceConfig.isActiveConsumerFailoverConsistentHashing()) {
            index = partitionIndex % consumersSize;
        } else {
            index = pickConsumerIndexWithBalancedHashing(consumersSize);
        }

        Consumer selectedConsumer = consumers.get(index);

        if (selectedConsumer == activeConsumer) {
            // Active consumer did not change. Do nothing at this point
            return false;
        } else {
            // If the active consumer is changed, send notification.
            activeConsumer = selectedConsumer;
            scheduleReadOnActiveConsumer();
            return true;
        }
    }

    private int peekConsumerIndexFromHashRing(NavigableMap<Integer, Integer> hashRing) {
        int hash = Murmur3Hash32.getInstance().makeHash(topicName);
        Map.Entry<Integer, Integer> ceilingEntry = hashRing.ceilingEntry(hash);
        return ceilingEntry != null ? ceilingEntry.getValue() : hashRing.firstEntry().getValue();
    }

    private NavigableMap<Integer, Integer> makeHashRing(int consumerSize) {
        NavigableMap<Integer, Integer> hashRing = new TreeMap<>();
        for (int i = 0; i < consumerSize; i++) {
            for (int j = 0; j < CONSUMER_CONSISTENT_HASH_REPLICAS; j++) {
                String key = consumers.get(i).consumerName() + j;
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                hashRing.put(hash, i);
            }
        }
        return Collections.unmodifiableNavigableMap(hashRing);
    }

    /**
     * Re-picks the active consumer when partitions are added to the partitioned topic, because consistent hashing
     * assigns the partitions by the partition count.
     */
    public synchronized void onPartitionsUpdate(int partitions) {
        if (partitions <= numPartitions) {
            return;
        }
        numPartitions = partitions;
        if (serviceConfig.isActiveConsumerFailoverConsistentHashing() && closeFuture == null && !consumers.isEmpty()) {
            pickAndScheduleActiveConsumer();
        }
    }

    /**
     * Consistent hashing selection for a partition of a partitioned topic, see {@link #balancedAssignment}.
     *
     * @return the index of the selected consumer
     */
    private int pickConsumerIndexWithBalancedHashing(int consumersSize) {
        String partitionedTopicName = TopicName.get(topicName).getPartitionedTopicName();
        // Partitions can only be added, so the largest count seen is the current one
        numPartitions = Math.max(numPartitions, subscription.getTopic().getBrokerService()
                .fetchPartitionedTopicMetadataIfCachedAndAsyncLoad(TopicName.get(partitionedTopicName)).partitions);
        // shortcut: until the partition count is cached, or when the table is too big, the partition uses the hash
        // ring until the next consumer change or partition update. Re-pick on metadata load if that shows up.
        if (numPartitions <= partitionIndex || (long) numPartitions * consumersSize > MAX_BALANCED_HASHING_PAIRS) {
            return peekConsumerIndexFromHashRing(makeHashRing(consumersSize));
        }
        List<String> consumerNames = new ArrayList<>(consumersSize);
        for (int i = 0; i < consumersSize; i++) {
            consumerNames.add(consumers.get(i).consumerName());
        }
        int partitions = numPartitions;
        return BALANCED_ASSIGNMENTS.get(List.of(partitionedTopicName, partitions, consumerNames),
                key -> balancedAssignment(partitionedTopicName, partitions, consumerNames))[partitionIndex];
    }

    /**
     * Assigns the partitions of a partitioned topic to consumers so that each consumer gets {@code numPartitions / n}
     * or one more partition, partitions of different topics land on different consumers, and adding or removing a
     * consumer moves few partitions.
     *
     * <p>Every (partition, consumer) pair has a hash score. Pairs are taken in descending score order, and a pair is
     * assigned when its partition is still unassigned and its consumer is under quota: every consumer gets
     * {@code numPartitions / n}, and only {@code numPartitions % n} consumers can get one more. Every partition
     * computes the same assignment on its own, so the partitions need no coordination.
     *
     * <p>The quotas apply to indexes in {@code consumerNames}. Balance across consumer instances requires
     * each index to identify the same logical consumer on every partition. Consumer names and instance ids establish
     * this correspondence after sorting. Legacy clients without instance ids must use distinct consumer names.
     *
     * @param consumerNames the candidate consumer names, in the same order for every partition
     * @return for each partition, the index in {@code consumerNames} of its consumer
     */
    static int[] balancedAssignment(String partitionedTopicName, int numPartitions, List<String> consumerNames) {
        int n = consumerNames.size();
        checkArgument(n > 0 && numPartitions > 0 && (long) numPartitions * n <= Integer.MAX_VALUE);
        long[] consumerHashes = new long[n];
        for (int c = 0; c < n; c++) {
            consumerHashes[c] = Murmur3_32Hash.getInstance()
                    .makeHash(consumerNames.get(c).getBytes(StandardCharsets.UTF_8));
        }
        int base = numPartitions / n;
        int extra = numPartitions % n;
        int extraUsed = 0;
        int[] loads = new int[n];
        long[] partitionHashes = new long[numPartitions];
        // The best (score << 32 | pairId) of every unassigned partition among the consumers with room left
        PriorityQueue<Long> candidates = new PriorityQueue<>(numPartitions, Comparator.reverseOrder());
        for (int p = 0; p < numPartitions; p++) {
            partitionHashes[p] = Murmur3_32Hash.getInstance().makeHash(
                    TopicName.getTopicPartitionNameString(partitionedTopicName, p).getBytes(StandardCharsets.UTF_8));
            candidates.add(bestCandidate(p, partitionHashes[p], consumerHashes, loads, base, extra > 0));
        }
        int[] assignment = new int[numPartitions];
        while (!candidates.isEmpty()) {
            int pairId = candidates.poll().intValue();
            int p = pairId / n;
            int c = pairId % n;
            if (!hasRoom(loads[c], base, extraUsed < extra)) {
                // Consumers only fill up, so the next best pair of the partition is its best among those with room
                candidates.add(bestCandidate(p, partitionHashes[p], consumerHashes, loads, base, extraUsed < extra));
                continue;
            }
            assignment[p] = c;
            if (loads[c]++ == base) {
                extraUsed++;
            }
        }
        return assignment;
    }

    private static boolean hasRoom(int load, int base, boolean extraLeft) {
        return load < base || (load == base && extraLeft);
    }

    private static long bestCandidate(int partition, long partitionHash, long[] consumerHashes, int[] loads,
                                      int base, boolean extraLeft) {
        long best = -1;
        for (int c = 0; c < consumerHashes.length; c++) {
            if (hasRoom(loads[c], base, extraLeft)) {
                long pairId = (long) partition * consumerHashes.length + c;
                best = Math.max(best, score(partitionHash, consumerHashes[c]) << 32 | pairId);
            }
        }
        return best;
    }

    // MurmurHash3 fmix64 of both hashes, cut to 31 bits so that (score << 32 | pairId) stays positive
    private static long score(long partitionHash, long consumerHash) {
        long h = partitionHash << 32 | consumerHash;
        h = (h ^ (h >>> 33)) * 0xff51afd7ed558ccdL;
        h = (h ^ (h >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return (h ^ (h >>> 33)) >>> 33;
    }

    public CompletableFuture<Void> addConsumer(Consumer consumer) {
        return internalAddConsumer(consumer, 0);
    }

    private synchronized CompletableFuture<Void> internalAddConsumer(Consumer consumer, int retryCount) {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn()
                    .attr("topic", this.topicName)
                    .attr("consumer", consumer)
                    .log("Dispatcher is already closed. Closing consumer");
            consumer.disconnect();
            return CompletableFuture.completedFuture(null);
        }

        if (subscriptionType == SubType.Exclusive && !consumers.isEmpty()) {
            Consumer actConsumer = getActiveConsumer();
            if (actConsumer != null) {
                final var callerThread = Thread.currentThread();
                return actConsumer.cnx().checkConnectionLiveness().thenCompose(actConsumerStillAlive -> {
                    if (actConsumerStillAlive.isEmpty() || actConsumerStillAlive.get()) {
                        return FutureUtil.failedFuture(new ConsumerBusyException("Exclusive consumer is already"
                                + " connected"));
                    } else if (retryCount >= MAX_RETRY_COUNT_FOR_ADD_CONSUMER_RACE) {
                        log.warn()
                                .attr("arg0", getName())
                                .attr("actConsumer", actConsumer)
                                .attr("consumer", consumer)
                                .log("The active consumer's connection is still inactive after all retries, skip "
                                        + "adding new consumer");
                        return FutureUtil.failedFuture(new ConsumerBusyException("Exclusive consumer is already"
                                + " connected after " + MAX_RETRY_COUNT_FOR_ADD_CONSUMER_RACE + " attempts"));
                    } else {
                        if (Thread.currentThread().equals(callerThread)) {
                            // A race condition happened in `ServerCnx#channelInactive`
                            // 1. `isActive` was set to false
                            // 2. `consumer.close()` is called
                            // We should wait until the consumer is closed, retry for some times
                            log.warn()
                                    .attr("arg0", getName())
                                    .attr("actConsumer", actConsumer)
                                    .log("race condition happened that cnx of the active consumer () is inactive "
                                            + "but it's not removed, retrying");
                            final var future = new CompletableFuture<Void>();
                            CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
                                    .execute(() -> future.complete(null));
                            return future.thenCompose(__ -> internalAddConsumer(consumer, retryCount + 1));
                        } else {
                            return internalAddConsumer(consumer, retryCount + 1);
                        }
                    }
                });
            } else {
                // It should never happen.

                return FutureUtil.failedFuture(new ConsumerBusyException("Active consumer is in a strange state."
                        + " Active consumer is null, but there are " + consumers.size() + " registered."));
            }
        }

        if (subscriptionType == SubType.Failover && isConsumersExceededOnSubscription()) {
            log.warn()
                    .attr("topic", this.topicName)
                    .log("Attempting to add consumer to subscription which reached max consumers limit");
            return FutureUtil.failedFuture(new ConsumerBusyException("Subscription reached max consumers limit"));
        }

        if (subscriptionType == SubType.Exclusive
                && consumer.getKeySharedMeta() != null
                && consumer.getKeySharedMeta().getHashRangesList() != null
                && consumer.getKeySharedMeta().getHashRangesList().size() > 0) {
            stickyKeyConsumerSelector = new HashRangeExclusiveStickyKeyConsumerSelector();
            stickyKeyConsumerSelector.addConsumer(consumer);
            isKeyHashRangeFiltered = true;
        } else {
            isKeyHashRangeFiltered = false;
        }

        if (consumers.isEmpty()) {
            isFirstRead = true;
            originalLedgerReadPosition = null;
        }

        consumers.add(consumer);

        if (!pickAndScheduleActiveConsumer()) {
            // the active consumer is not changed
            Consumer currentActiveConsumer = getActiveConsumer();
            if (null == currentActiveConsumer) {
                log.debug().attr("consumer", consumer).log("Current active consumer disappears while adding consumer");
            } else {
                consumer.notifyActiveConsumerChange(currentActiveConsumer);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        log.info().attr("consumer", consumer).log("Removing consumer");
        if (!consumers.remove(consumer)) {
            throw new ServerMetadataException("Consumer was not connected");
        }

        if (consumers.isEmpty()) {
            activeConsumer = null;
        }

        if (closeFuture == null && !consumers.isEmpty()) {
            pickAndScheduleActiveConsumer();
            return;
        }

        cancelPendingRead();

        if (consumers.isEmpty() && closeFuture != null && !closeFuture.isDone()) {
            // Control reaches here only when closeFuture is created
            // and no more connected consumers left.
            closeFuture.complete(null);
        }
    }

    /**
     * Handle unsubscribe command from the client API For failover subscription, if consumer is connected consumer, we
     * can unsubscribe.
     *
     * @param consumer
     *            Calling consumer object
     */
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return (consumers.size() == 1) && Objects.equals(consumer, activeConsumer);
    }

    @Override
    public CompletableFuture<Void> close(boolean disconnectConsumers,
                                         Optional<BrokerLookupData> assignedBrokerLookupData) {
        IS_CLOSED_UPDATER.set(this, TRUE);
        getRateLimiter().ifPresent(DispatchRateLimiter::close);
        return disconnectConsumers
                ? disconnectAllConsumers(false, assignedBrokerLookupData) : CompletableFuture.completedFuture(null);
    }

    public boolean isClosed() {
        return isClosed == TRUE;
    }

    /**
     * Disconnect all consumers on this dispatcher (server side close). This triggers channelInactive on the inbound
     * handler which calls dispatcher.removeConsumer(), where the closeFuture is completed.
     *
     * @param isResetCursor
     *              Specifies if the cursor has been reset.
     * @param assignedBrokerLookupData
     *              Optional target broker redirect information. Allows the consumer to quickly reconnect to a broker
     *              during bundle unloading.
     *
     * @return CompletableFuture indicating the completion of the operation.
     */
    @Override
    public synchronized CompletableFuture<Void> disconnectAllConsumers(
            boolean isResetCursor, Optional<BrokerLookupData> assignedBrokerLookupData) {
        closeFuture = new CompletableFuture<>();

        if (!consumers.isEmpty()) {
            consumers.forEach(consumer -> consumer.disconnect(isResetCursor, assignedBrokerLookupData));
            cancelPendingRead();
        } else {
            // no consumer connected, complete disconnect immediately
            closeFuture.complete(null);
        }
        return closeFuture;
    }

    public synchronized CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        closeFuture = new CompletableFuture<>();
        if (activeConsumer != null) {
            activeConsumer.disconnect(isResetCursor);
        }
        closeFuture.complete(null);
        return closeFuture;
    }

    @Override
    public synchronized void resetCloseFuture() {
        closeFuture = null;
    }

    public void reset() {
        resetCloseFuture();
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    public SubType getType() {
        return subscriptionType;
    }

    public Consumer getActiveConsumer() {
        return activeConsumer;
    }

    @Override
    public List<Consumer> getConsumers() {
        return consumers;
    }

    public boolean isConsumerConnected() {
        return activeConsumer != null;
    }
}
