/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.util.Random;

import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageRouter;

public class SinglePartitionMessageRouterImpl implements MessageRouter {

    private final int partitionIndex;
    private final int numPartitions;

    public SinglePartitionMessageRouterImpl(int numPartitions) {
        this.partitionIndex = new Random().nextInt(numPartitions);
        this.numPartitions = numPartitions;
    }

    @Override
    public int choosePartition(Message msg) {
        // If the message has a key, it supersedes the single partition routing policy
        if (msg.hasKey()) {
            return ((msg.getKey().hashCode() & Integer.MAX_VALUE) % numPartitions);
        }

        return partitionIndex;
    }

}
