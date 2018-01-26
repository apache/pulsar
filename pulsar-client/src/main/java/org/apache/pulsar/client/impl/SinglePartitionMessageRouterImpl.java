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

import java.nio.charset.StandardCharsets;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

public class SinglePartitionMessageRouterImpl extends MessageRouterBase {

    private final int partitionIndex;

    public SinglePartitionMessageRouterImpl(int partitionIndex, boolean useMurmurHash) {
        super(useMurmurHash);
        this.partitionIndex = partitionIndex;
    }

    @Override
    public int choosePartition(Message msg, TopicMetadata metadata) {
        // If the message has a key, it supersedes the single partition routing policy
        if (msg.hasKey()) {
            if (useMurmurHash) {
                HashFunction hf = Hashing.murmur3_32(0);
                HashCode hc = hf.hashString(msg.getKey(), StandardCharsets.UTF_8);
                long l = Integer.toUnsignedLong(hc.asInt());
                return (int) (l % metadata.numPartitions());
            }
            else {
                return ((msg.getKey().hashCode() & Integer.MAX_VALUE) % metadata.numPartitions());
            }
        }

        return partitionIndex;
    }

}
