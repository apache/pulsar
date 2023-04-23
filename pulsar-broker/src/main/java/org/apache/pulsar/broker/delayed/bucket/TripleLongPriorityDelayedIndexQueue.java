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
package org.apache.pulsar.broker.delayed.bucket;

import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;

@NotThreadSafe
class TripleLongPriorityDelayedIndexQueue implements DelayedIndexQueue {

    private final TripleLongPriorityQueue queue;

    private TripleLongPriorityDelayedIndexQueue(TripleLongPriorityQueue queue) {
        this.queue = queue;
    }

    public static TripleLongPriorityDelayedIndexQueue wrap(TripleLongPriorityQueue queue) {
        return new TripleLongPriorityDelayedIndexQueue(queue);
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public DelayedIndex peek() {
        DelayedIndex delayedIndex = new DelayedIndex().setTimestamp(queue.peekN1())
                .setLedgerId(queue.peekN2()).setEntryId(queue.peekN3());
        return delayedIndex;
    }

    @Override
    public DelayedIndex pop() {
        DelayedIndex peek = peek();
        queue.pop();
        return peek;
    }

    @Override
    public void popToObject(DelayedIndex delayedIndex) {
        delayedIndex.setTimestamp(queue.peekN1())
                .setLedgerId(queue.peekN2()).setEntryId(queue.peekN3());
        queue.pop();
    }

    @Override
    public long peekTimestamp() {
        return queue.peekN1();
    }
}
