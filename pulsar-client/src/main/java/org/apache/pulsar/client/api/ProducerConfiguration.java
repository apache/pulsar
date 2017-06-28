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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.RoundRobinPartitionMessageRouterImpl;
import org.apache.pulsar.client.impl.SinglePartitionMessageRouterImpl;

import com.google.common.base.Objects;

/**
 * Producer's configuration
 *
 */
public class ProducerConfiguration implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private long sendTimeoutMs = 30000;
    private boolean blockIfQueueFull = false;
    private int maxPendingMessages = 1000;
    private MessageRoutingMode messageRouteMode = MessageRoutingMode.SinglePartition;
    private MessageRouter customMessageRouter = null;
    private long batchingMaxPublishDelayMs = 10;
    private int batchingMaxMessages = 1000;
    private boolean batchingEnabled = false; // disabled by default

    private CompressionType compressionType = CompressionType.NONE;

    public enum MessageRoutingMode {
        SinglePartition, RoundRobinPartition, CustomPartition
    }

    /**
     * @return the message send timeout in ms
     */
    public long getSendTimeoutMs() {
        return sendTimeoutMs;
    }

    /**
     * Set the send timeout <i>(default: 30 seconds)</i>
     * <p>
     * If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
     *
     * @param sendTimeout
     *            the send timeout
     * @param unit
     *            the time unit of the {@code sendTimeout}
     */
    public ProducerConfiguration setSendTimeout(int sendTimeout, TimeUnit unit) {
        checkArgument(sendTimeout >= 0);
        this.sendTimeoutMs = unit.toMillis(sendTimeout);
        return this;
    }

    /**
     * @return the maximum number of messages allowed in the outstanding messages queue for the producer
     */
    public int getMaxPendingMessages() {
        return maxPendingMessages;
    }

    /**
     * Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
     * <p>
     * When the queue is full, by default, all calls to {@link Producer#send} and {@link Producer#sendAsync}
     * will fail unless blockIfQueueFull is set to true. Use {@link #setBlockIfQueueFull} to change the blocking behavior.
     *
     * @param maxPendingMessages
     * @return
     */
    public ProducerConfiguration setMaxPendingMessages(int maxPendingMessages) {
        checkArgument(maxPendingMessages > 0);
        this.maxPendingMessages = maxPendingMessages;
        return this;
    }

    /**
     *
     * @return whether the producer will block {@link Producer#send} and {@link Producer#sendAsync} operations when the
     *         pending queue is full
     */
    public boolean getBlockIfQueueFull() {
        return blockIfQueueFull;
    }

    /**
     * Set whether the {@link Producer#send} and {@link Producer#sendAsync} operations should block when the outgoing
     * message queue is full.
     * <p>
     * Default is <code>false</code>. If set to <code>false</code>, send operations will immediately fail with
     * {@link ProducerQueueIsFullError} when there is no space left in pending queue.
     *
     * @param blockIfQueueFull
     *            whether to block {@link Producer#send} and {@link Producer#sendAsync} operations on queue full
     * @return
     */
    public ProducerConfiguration setBlockIfQueueFull(boolean blockIfQueueFull) {
        this.blockIfQueueFull = blockIfQueueFull;
        return this;
    }

    /**
     * Set the message routing mode for the partitioned producer
     *
     * @param mode
     * @return
     */
    public ProducerConfiguration setMessageRoutingMode(MessageRoutingMode messageRouteMode) {
        checkNotNull(messageRouteMode);
        this.messageRouteMode = messageRouteMode;
        return this;
    }

    /**
     * Get the message routing mode for the partitioned producer
     *
     * @return
     */
    public MessageRoutingMode getMessageRoutingMode() {
        return messageRouteMode;
    }

    /**
     * Set the compression type for the producer.
     * <p>
     * By default, message payloads are not compressed. Supported compression types are:
     * <ul>
     * <li><code>CompressionType.LZ4</code></li>
     * <li><code>CompressionType.ZLIB</code></li>
     * </ul>
     *
     * @param compressionType
     * @return
     *
     * @since 1.0.28 <br>
     *        Make sure all the consumer applications have been updated to use this client version, before starting to
     *        compress messages.
     */
    public ProducerConfiguration setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    /**
     * @return the configured compression type for this producer
     */
    public CompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Set a custom message routing policy by passing an implementation of MessageRouter
     *
     *
     * @param messageRouter
     */
    public ProducerConfiguration setMessageRouter(MessageRouter messageRouter) {
        checkNotNull(messageRouter);
        setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        customMessageRouter = messageRouter;
        return this;
    }

    /**
     * Get the message router object
     *
     * @return
     */
    public MessageRouter getMessageRouter(int numPartitions) {
        MessageRouter messageRouter;

        switch (messageRouteMode) {
        case CustomPartition:
            checkNotNull(customMessageRouter);
            messageRouter = customMessageRouter;
            break;
        case RoundRobinPartition:
            messageRouter = new RoundRobinPartitionMessageRouterImpl(numPartitions);
            break;
        case SinglePartition:
        default:
            messageRouter = new SinglePartitionMessageRouterImpl(numPartitions);
        }

        return messageRouter;
    }

    /**
     * @ return if batch messages are enabled
     */

    public boolean getBatchingEnabled() {
        return batchingEnabled;
    }

    /**
     * Control whether automatic batching of messages is enabled for the producer. <i>default: false [No batching]</i>
     *
     * When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
     * broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
     * messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
     * contents.
     *
     * When enabled default batch delay is set to 10 ms and default batch size is 1000 messages
     * 
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit)
     * @since 1.0.36 <br>
     *        Make sure all the consumer applications have been updated to use this client version, before starting to
     *        batch messages.
     */

    public ProducerConfiguration setBatchingEnabled(boolean batchMessagesEnabled) {
        this.batchingEnabled = batchMessagesEnabled;
        return this;
    }

    /**
     * 
     * @return the batch time period in ms.
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit)
     */
    public long getBatchingMaxPublishDelayMs() {
        return batchingMaxPublishDelayMs;
    }

    /**
     * Set the time period within which the messages sent will be batched <i>default: 10ms</i> if batch messages are
     * enabled. If set to a non zero value, messages will be queued until this time interval or until
     * 
     * @see ProducerConfiguration#batchingMaxMessages threshold is reached; all messages will be published as a single
     *      batch message. The consumer will be delivered individual messages in the batch in the same order they were
     *      enqueued
     * @since 1.0.36 <br>
     *        Make sure all the consumer applications have been updated to use this client version, before starting to
     *        batch messages.
     * @param batchDelay
     *            the batch delay
     * @param timeUnit
     *            the time unit of the {@code batchDelay}
     * @return
     */
    public ProducerConfiguration setBatchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit) {
        long delayInMs = timeUnit.toMillis(batchDelay);
        checkArgument(delayInMs >= 1, "configured value for batch delay must be at least 1ms");
        this.batchingMaxPublishDelayMs = delayInMs;
        return this;
    }

    /**
     * 
     * @return the maximum number of messages permitted in a batch.
     */
    public int getBatchingMaxMessages() {
        return batchingMaxMessages;
    }

    /**
     * Set the maximum number of messages permitted in a batch. <i>default: 1000</i> If set to a value greater than 1,
     * messages will be queued until this threshold is reached or batch interval has elapsed
     * 
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit) All messages in batch will be published as
     *      a single batch message. The consumer will be delivered individual messages in the batch in the same order
     *      they were enqueued
     * @param batchMessagesMaxMessagesPerBatch
     *            maximum number of messages in a batch
     * @return
     */
    public ProducerConfiguration setBatchingMaxMessages(int batchMessagesMaxMessagesPerBatch) {
        checkArgument(batchMessagesMaxMessagesPerBatch > 0);
        this.batchingMaxMessages = batchMessagesMaxMessagesPerBatch;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ProducerConfiguration) {
            ProducerConfiguration other = (ProducerConfiguration) obj;
            return Objects.equal(this.sendTimeoutMs, other.sendTimeoutMs)
                    && Objects.equal(maxPendingMessages, other.maxPendingMessages)
                    && Objects.equal(this.messageRouteMode, other.messageRouteMode);
        }

        return false;
    }
}
