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
package org.apache.pulsar.client.impl;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;

public class RoundRobinPartitionMessageRouterImpl implements MessageRouter {

    private static final AtomicIntegerFieldUpdater<RoundRobinPartitionMessageRouterImpl> PARTITION_INDEX_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RoundRobinPartitionMessageRouterImpl.class, "partitionIndex");
    private volatile int partitionIndex = 0;
    private final int numPartitions;

    public RoundRobinPartitionMessageRouterImpl(int numPartitions) {
        this.numPartitions = numPartitions;
        PARTITION_INDEX_UPDATER.set(this, 0);
    }

    @Override
    public int choosePartition(Message msg) {
        // If the message has a key, it supersedes the round robin routing policy
        if (msg.hasKey()) {
            return ((msg.getKey().hashCode() & Integer.MAX_VALUE) % numPartitions);
        }
        return ((PARTITION_INDEX_UPDATER.getAndIncrement(this) & Integer.MAX_VALUE) % numPartitions);
    }

}
