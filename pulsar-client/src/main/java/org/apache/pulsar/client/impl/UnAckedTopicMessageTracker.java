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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;

import java.util.Iterator;

public class UnAckedTopicMessageTracker extends UnAckedMessageTracker {

    public UnAckedTopicMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        super(client, consumerBase, ackTimeoutMillis);
    }

    public UnAckedTopicMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis, long tickDurationMillis) {
        super(client, consumerBase, ackTimeoutMillis, tickDurationMillis);
    }

    public int removeTopicMessages(String topicName) {
        writeLock.lock();
        try {
            int removed = 0;
            Iterator<MessageId> iterator = messageIdPartitionMap.keySet().iterator();
            while (iterator.hasNext()) {
                MessageId messageId = iterator.next();
                if (messageId instanceof TopicMessageIdImpl &&
                        ((TopicMessageIdImpl)messageId).getTopicPartitionName().contains(topicName)) {
                    ConcurrentOpenHashSet<MessageId> exist = messageIdPartitionMap.get(messageId);
                    if (exist != null) {
                        exist.remove(messageId);
                    }
                    iterator.remove();
                    removed ++;
                }
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

}
