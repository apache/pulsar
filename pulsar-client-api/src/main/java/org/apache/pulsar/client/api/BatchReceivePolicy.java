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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Configuration for message batch receive {@link Consumer#batchReceive()} {@link Consumer#batchReceiveAsync()}.
 *
 * <p>Batch receive policy can limit the number and bytes of messages in a single batch, and can specify a timeout
 * for waiting for enough messages for this batch.
 *
 * <p>This batch receive will be completed as long as any one of the
 * conditions(has enough number of messages, has enough of size of messages, wait timeout) is met.
 *
 * <p>Examples:
 * 1.If set maxNumMessages = 10, maxSizeOfMessages = 1MB and without timeout, it
 * means {@link Consumer#batchReceive()} will always wait until there is enough messages.
 * 2.If set maxNumberOfMessages = 0, maxNumBytes = 0 and timeout = 100ms, it
 * means {@link Consumer#batchReceive()} will waiting for 100ms whether or not there is enough messages.
 *
 * <p>Note:
 * Must specify messages limitation(maxNumMessages, maxNumBytes) or wait timeout.
 * Otherwise, {@link Messages} ingest {@link Message} will never end.
 *
 * @since 2.4.1
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BatchReceivePolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Default batch receive policy.
     *
     * <p>Max number of messages: no limit
     * Max number of bytes: 10MB
     * Timeout: 100ms<p/>
     */
    public static final BatchReceivePolicy DEFAULT_POLICY = new BatchReceivePolicy(
            -1, 10 * 1024 * 1024, 100, TimeUnit.MILLISECONDS);

    private BatchReceivePolicy(int maxNumMessages, int maxNumBytes, int timeout, TimeUnit timeoutUnit) {
        this.maxNumMessages = maxNumMessages;
        this.maxNumBytes = maxNumBytes;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    /**
     * Max number of messages for a single batch receive, 0 or negative means no limit.
     */
    private final int maxNumMessages;

    /**
     * Max bytes of messages for a single batch receive, 0 or negative means no limit.
     */
    private final int maxNumBytes;

    /**
     * timeout for waiting for enough messages(enough number or enough bytes).
     */
    private final int timeout;
    private final TimeUnit timeoutUnit;

    public void verify() {
        if (maxNumMessages <= 0 && maxNumBytes <= 0 && timeout <= 0) {
            throw new IllegalArgumentException("At least "
                    + "one of maxNumMessages, maxNumBytes, timeout must be specified.");
        }
        if (timeout > 0 && timeoutUnit == null) {
            throw new IllegalArgumentException("Must set timeout unit for timeout.");
        }
    }

    public long getTimeoutMs() {
        return (timeout > 0 && timeoutUnit != null) ? timeoutUnit.toMillis(timeout) : 0L;
    }

    public int getMaxNumMessages() {
        return maxNumMessages;
    }

    public int getMaxNumBytes() {
        return maxNumBytes;
    }

    /**
     * Builder of BatchReceivePolicy.
     */
    public static class Builder {

        private int maxNumMessages;
        private int maxNumBytes;
        private int timeout;
        private TimeUnit timeoutUnit;

        public Builder maxNumMessages(int maxNumMessages) {
            this.maxNumMessages = maxNumMessages;
            return this;
        }

        public Builder maxNumBytes(int maxNumBytes) {
            this.maxNumBytes = maxNumBytes;
            return this;
        }

        public Builder timeout(int timeout, TimeUnit timeoutUnit) {
            this.timeout = timeout;
            this.timeoutUnit = timeoutUnit;
            return this;
        }

        public BatchReceivePolicy build() {
            return new BatchReceivePolicy(maxNumMessages, maxNumBytes, timeout, timeoutUnit);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BatchReceivePolicy{"
                + "maxNumMessages=" + maxNumMessages
                + ", maxNumBytes=" + maxNumBytes
                + ", timeout=" + timeout
                + ", timeoutUnit=" + timeoutUnit
                + '}';
    }
}
