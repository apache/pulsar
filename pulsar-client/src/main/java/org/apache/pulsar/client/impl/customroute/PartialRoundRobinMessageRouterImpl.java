/**
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
package org.apache.pulsar.client.impl.customroute;

import static org.apache.pulsar.client.util.MathUtils.signSafeMod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

public class PartialRoundRobinMessageRouterImpl implements MessageRouter {
    private final int numPartitionsLimit;
    private final List<Integer> partialList = new CopyOnWriteArrayList<>();
    private static final AtomicIntegerFieldUpdater<PartialRoundRobinMessageRouterImpl> PARTITION_INDEX_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PartialRoundRobinMessageRouterImpl.class, "partitionIndex");
    @SuppressWarnings("unused")
    private volatile int partitionIndex = 0;

    public PartialRoundRobinMessageRouterImpl(final int numPartitionsLimit) {
        if (numPartitionsLimit < 1) {
            throw new IllegalArgumentException("numPartitionsLimit should be greater than or equal to 1");
        }
        this.numPartitionsLimit = numPartitionsLimit;
    }

    /**
     * Choose a partition based on the topic metadata.
     * Key hash routing isn't supported.
     *
     * @param msg message
     * @param metadata topic metadata
     * @return the partition to route the message.
     */
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        final List<Integer> newPartialList = new ArrayList<>(getOrCreatePartialList(metadata));
        return newPartialList
                .get(signSafeMod(PARTITION_INDEX_UPDATER.getAndIncrement(this), newPartialList.size()));
    }

    private List<Integer> getOrCreatePartialList(TopicMetadata metadata) {
        if (partialList.isEmpty()
                || partialList.size() < numPartitionsLimit && partialList.size() < metadata.numPartitions()) {
            synchronized (this) {
                if (partialList.isEmpty()) {
                    partialList.addAll(IntStream.range(0, metadata.numPartitions()).boxed()
                            .collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
                                Collections.shuffle(list);
                                return list.stream();
                            })).limit(numPartitionsLimit).collect(Collectors.toList()));
                } else if (partialList.size() < numPartitionsLimit && partialList.size() < metadata.numPartitions()) {
                    partialList.addAll(IntStream.range(0, metadata.numPartitions()).boxed()
                            .filter(e -> !partialList.contains(e))
                            .collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
                                Collections.shuffle(list);
                                return list.stream();
                            })).limit(numPartitionsLimit - partialList.size()).collect(Collectors.toList()));
                }
            }
        }
        return partialList;
    }
}
