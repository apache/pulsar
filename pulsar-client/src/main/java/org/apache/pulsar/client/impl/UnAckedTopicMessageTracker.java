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

import static com.google.common.base.Preconditions.checkState;

public class UnAckedTopicMessageTracker extends UnAckedMessageTracker {

    public UnAckedTopicMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        super(client, consumerBase, ackTimeoutMillis);
    }

    public int removeTopicMessages(String topicName) {
        readLock.lock();
        try {
            int currentSetRemovedMsgCount = currentSet.removeIf(m -> {
                checkState(m instanceof TopicMessageIdImpl,
                    "message should be of type TopicMessageIdImpl");
                return ((TopicMessageIdImpl)m).getTopicPartitionName().contains(topicName);
            });
            int oldSetRemovedMsgCount = oldOpenSet.removeIf(m -> {
                checkState(m instanceof TopicMessageIdImpl,
                    "message should be of type TopicMessageIdImpl");
                return ((TopicMessageIdImpl)m).getTopicPartitionName().contains(topicName);
            });

            return currentSetRemovedMsgCount + oldSetRemovedMsgCount;
        } finally {
            readLock.unlock();
        }
    }

}
