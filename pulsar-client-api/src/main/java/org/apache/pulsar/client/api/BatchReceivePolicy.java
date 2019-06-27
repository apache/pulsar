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
package org.apache.pulsar.client.api;

import lombok.Builder;
import lombok.Data;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for message batch receive {@link Consumer#batchReceive()} {@link Consumer#batchReceiveAsync()}.
 *
 * Batch receive policy can limit the number and size of messages in a single batch, and can specify a timeout
 * for waiting for enough messages for this batch.
 *
 * This batch receive will be completed as long as any one of the
 * conditions(has enough number of messages, has enough of size of messages, wait timeout) is met.
 *
 * Examples:
 *
 * 1.If set maxNumberOfMessages = 10, maxSizeOfMessages = 1MB and without timeout, it
 * means {@link Consumer#batchReceive()} will always wait until there is enough messages.
 *
 * 2.If set maxNumberOfMessages = 0, maxSizeOfMessages = 0 and timeout = 100ms, it
 * means {@link Consumer#batchReceive()} will waiting for 100ms whether or not there is enough messages.
 *
 * Note:
 * Must specify messages limitation(maxNumberOfMessages, maxSizeOfMessages) or wait timeout.
 * Otherwise, {@link Messages} ingest {@link Message} will never end.
 *
 * @since 2.4.1
 */
@Builder
@Data
public class BatchReceivePolicy {

    /**
     * Default batch receive policy
     *
     * Max number of messages: 100
     * Max size of messages: 10MB
     * Timeout: 100ms
     */
    public static final BatchReceivePolicy DEFAULT_POLICY = new BatchReceivePolicy(
            100, 1024 * 1024 * 10, 100, TimeUnit.MILLISECONDS);

    /**
     * Max number of message for a single batch receive, 0 or negative means no limit.
     */
    private int maxNumberOfMessages;

    /**
     * Max size of message for a single batch receive, 0 or negative means no limit.
     */
    private long maxSizeOfMessages;

    /**
     * timeout for waiting for enough messages(enough number or enough size).
     */
    private int timeout;
    private TimeUnit timeoutUnit;

    public void verify() {
        if (maxNumberOfMessages <= 0 && maxSizeOfMessages <= 0 && timeout <= 0) {
            throw new IllegalArgumentException("At least " +
                    "one of maxNumberOfMessages, maxSizeOfMessages, timeout must be specified.");
        }
        if (timeout > 0 && timeoutUnit == null) {
            throw new IllegalArgumentException("Must set timeout unit for timeout.");
        }
    }

    public long getTimeoutMs() {
        return (timeout > 0 && timeoutUnit != null) ? timeoutUnit.toMillis(timeout) : 0L;
    }
}
