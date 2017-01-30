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

#ifndef CONSUMER_HPP_
#define CONSUMER_HPP_

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <pulsar/Message.h>
#include <pulsar/Result.h>

#pragma GCC visibility push(default)

class PulsarFriend;

namespace pulsar {

class Consumer;

/// Callback definition for non-data operation
typedef boost::function<void(Result result)> ResultCallback;

/// Callback definition for MessageListener
typedef boost::function<void(Consumer consumer, const Message& msg)> MessageListener;

enum ConsumerType {
    /**
     * There can be only 1 consumer on the same topic with the same consumerName
     */
    ConsumerExclusive,

    /**
     * Multiple consumers will be able to use the same consumerName and the messages
     *  will be dispatched according to a round-robin rotation between the connected consumers
     */
    ConsumerShared,

    /** Only one consumer is active on the subscription; Subscription can have N consumers
     *  connected one of which will get promoted to master if the current master becomes inactive
     */

    ConsumerFailover
};

/**
 * Class specifying the configuration of a consumer.
 */
class ConsumerConfiguration {
 public:
    ConsumerConfiguration();
    ~ConsumerConfiguration();
    ConsumerConfiguration(const ConsumerConfiguration&);
    ConsumerConfiguration& operator=(const ConsumerConfiguration&);

    /**
    * Specify the consumer type. The consumer type enables
    * specifying the type of subscription. In Exclusive subscription,
    * only a single consumer is allowed to attach to the subscription. Other consumers
    * will get an error message. In Shared subscription, multiple consumers will be
    * able to use the same subscription name and the messages will be dispatched in a
    * round robin fashion. In Failover subscription, a master-slave subscription model
    * allows for multiple consumers to attach to a single subscription, though only one
    * of them will be “master” at a given time. Only the master consumer will receive
    * messages. When the master gets disconnected, one among the slaves will be promoted
    * to master and will start getting messages.
    */
    ConsumerConfiguration& setConsumerType(ConsumerType consumerType);
    ConsumerType getConsumerType() const;

    /**
     * A message listener enables your application to configure how to process
     * and acknowledge messages delivered. A listener will be called in order
     * for every message received.
     */
    ConsumerConfiguration& setMessageListener(MessageListener messageListener);
    MessageListener getMessageListener() const;
    bool hasMessageListener() const;

    /**
     * Sets the size of the consumer receive queue.
     *
     * The consumer receive queue controls how many messages can be accumulated by the Consumer before the
     * application calls receive(). Using a higher value could potentially increase the consumer throughput
     * at the expense of bigger memory utilization.
     *
     * Setting the consumer queue size as zero decreases the throughput of the consumer, by disabling pre-fetching of
     * messages. This approach improves the message distribution on shared subscription, by pushing messages only to
     * the consumers that are ready to process them. Neither receive with timeout nor Partitioned Topics can be
     * used if the consumer queue size is zero. The receive() function call should not be interrupted when
     * the consumer queue size is zero.
     *
     * Default value is 1000 messages and should be good for most use cases.
     *
     * @param size
     *            the new receiver queue size value
     */
    void setReceiverQueueSize(int size);
    int getReceiverQueueSize() const;

    void setConsumerName(const std::string&);
    const std::string& getConsumerName() const;

    /**
     * Set the timeout in milliseconds for unacknowledged messages, the timeout needs to be greater than
     * 10 seconds. An Exception is thrown if the given value is less than 10000 (10 seconds).
     * If a successful acknowledgement is not sent within the timeout all the unacknowledged messages are
     * redelivered.
     * @param timeout in milliseconds
     */
    void setUnAckedMessagesTimeoutMs(const uint64_t milliSeconds);

    /**
     * @return the configured timeout in milliseconds for unacked messages.
     */
    long getUnAckedMessagesTimeoutMs() const;
 private:
    struct Impl;
    boost::shared_ptr<Impl> impl_;
};

class ConsumerImplBase;

/**
 *
 */
class Consumer {
 public:
    /**
     * Construct an uninitialized consumer object
     */
    Consumer();

    /**
     * @return the topic this consumer is subscribed to
     */
    const std::string& getTopic() const;

    /**
     * @return the consumer name
     */
    const std::string& getSubscriptionName() const;

    /**
     * Unsubscribe the current consumer from the topic.
     *
     * This method will block until the operation is completed. Once the consumer is
     * unsubscribed, no more messages will be received and subsequent new messages
     * will not be retained for this consumer.
     *
     * This consumer object cannot be reused.
     *
     * @see asyncUnsubscribe
     * @return Result::ResultOk if the unsubscribe operation completed successfully
     * @return Result::ResultError if the unsubscribe operation failed
     */
    Result unsubscribe();

    /**
     * Asynchronously unsubscribe the current consumer from the topic.
     *
     * This method will block until the operation is completed. Once the consumer is
     * unsubscribed, no more messages will be received and subsequent new messages
     * will not be retained for this consumer.
     *
     * This consumer object cannot be reused.
     *
     * @param callback the callback to get notified when the operation is complete
     */
    void unsubscribeAsync(ResultCallback callback);

    /**
     * Receive a single message.
     *
     * If a message is not immediately available, this method will block until a new
     * message is available.
     *
     * @param msg a non-const reference where the received message will be copied
     * @return ResultOk when a message is received
     * @return ResultInvalidConfiguration if a message listener had been set in the configuration
     */
    Result receive(Message& msg);

    /**
     *
     * @param msg a non-const reference where the received message will be copied
     * @param timeoutMs the receive timeout in milliseconds
     * @return ResultOk if a message was received
     * @return ResultTimeout if the receive timeout was triggered
     * @return ResultInvalidConfiguration if a message listener had been set in the configuration
     */
    Result receive(Message& msg, int timeoutMs);

    /**
     * Acknowledge the reception of a single message.
     *
     * This method will block until an acknowledgement is sent to the broker. After
     * that, the message will not be re-delivered to this consumer.
     *
     * @see asyncAcknowledge
     * @param message the message to acknowledge
     * @return ResultOk if the message was successfully acknowledged
     * @return ResultError if there was a failure
     */
    Result acknowledge(const Message& message);
    Result acknowledge(const MessageId& messageId);

    /**
     * Asynchronously acknowledge the reception of a single message.
     *
     * This method will initiate the operation and return immediately. The provided callback
     * will be triggered when the operation is complete.
     *
     * @param message the message to acknowledge
     * @param callback callback that will be triggered when the message has been acknowledged
     */
    void acknowledgeAsync(const Message& message, ResultCallback callback);
    void acknowledgeAsync(const MessageId& messageID, ResultCallback callback);

    /**
     * Acknowledge the reception of all the messages in the stream up to (and including)
     * the provided message.
     *
     * This method will block until an acknowledgement is sent to the broker. After
     * that, the messages will not be re-delivered to this consumer.
     *
     * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * It's equivalent to calling asyncAcknowledgeCumulative(const Message&, ResultCallback) and
     * waiting for the callback to be triggered.
     *
     * @param message the last message in the stream to acknowledge
     * @return ResultOk if the message was successfully acknowledged. All previously delivered messages for this topic are also acknowledged.
     * @return ResultError if there was a failure
     */
    Result acknowledgeCumulative(const Message& message);
    Result acknowledgeCumulative(const MessageId& messageId);

    /**
     * Asynchronously acknowledge the reception of all the messages in the stream up to (and
     * including) the provided message.
     *
     * This method will initiate the operation and return immediately. The provided callback
     * will be triggered when the operation is complete.
     *
     * @param message the message to acknowledge
     * @param callback callback that will be triggered when the message has been acknowledged
     */
    void acknowledgeCumulativeAsync(const Message& message, ResultCallback callback);
    void acknowledgeCumulativeAsync(const MessageId& messageId, ResultCallback callback);

    Result close();

    void closeAsync(ResultCallback callback);

    /*
     * Pause receiving messages via the messageListener, till resumeMessageListener() is called.
     */
    Result pauseMessageListener();

    /*
     * Resume receiving the messages via the messageListener.
     * Asynchronously receive all the messages enqueued from time pauseMessageListener() was called.
     */
    Result resumeMessageListener();

    /**
     * Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
     * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
     * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
     * breaks, the messages are redelivered after reconnect.
     */
    void redeliverUnacknowledgedMessages();

 private:
    typedef boost::shared_ptr<ConsumerImplBase> ConsumerImplBasePtr;
    friend class PulsarFriend;
    ConsumerImplBasePtr impl_;
    explicit Consumer(ConsumerImplBasePtr);

    friend class PartitionedConsumerImpl;
    friend class ConsumerImpl;
    friend class ClientImpl;
    friend class ConsumerTest;
};

}

#pragma GCC visibility pop

#endif /* CONSUMER_HPP_ */
