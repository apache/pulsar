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

import io.netty.util.Timeout;
import org.apache.pulsar.client.api.MessageId;

import java.util.Iterator;

public class WheelTimerUnAckedTopicMessageTracker extends WheelTimerUnAckedMessageTracker {

    public WheelTimerUnAckedTopicMessageTracker(ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        super(consumerBase, ackTimeoutMillis);
    }

    public int removeTopicMessages(String topicName) {
        writeLock.lock();
        try {
            int removed = 0;
            Iterator<MessageId> iterator = timeoutMap.keySet().iterator();
            while (iterator.hasNext()) {
                MessageId maybeRemove = iterator.next();
                if (maybeRemove instanceof TopicMessageIdImpl &&
                        ((TopicMessageIdImpl)maybeRemove).getTopicPartitionName().contains(topicName)) {
                    Timeout timeout = timeoutMap.get(maybeRemove);
                    if (timeout != null) {
                        timeout.cancel();
                    }
                    iterator.remove();
                    removed++;
                }
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }
}
