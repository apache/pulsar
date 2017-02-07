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

#ifndef PRODUCER_HPP_
#define PRODUCER_HPP_

#include <pulsar/Result.h>
#include <pulsar/Message.h>
#include <pulsar/MessageRoutingPolicy.h>

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <stdint.h>

#pragma GCC visibility push(default)

class PulsarFriend;

namespace pulsar {

typedef boost::function<void(Result, const Message& msg)> SendCallback;
typedef boost::function<void(Result)> CloseCallback;

enum CompressionType {
    CompressionNone = 0,
    CompressionLZ4  = 1,
    CompressionZLib = 2
};

/**
 * Class that holds the configuration for a producer
 */
class ProducerConfiguration {
 public:
    enum PartitionsRoutingMode {
        UseSinglePartition,
        RoundRobinDistribution,
        CustomPartition
    };
    ProducerConfiguration();
    ~ProducerConfiguration();
    ProducerConfiguration(const ProducerConfiguration&);
    ProducerConfiguration& operator=(const ProducerConfiguration&);

    ProducerConfiguration& setSendTimeout(int sendTimeoutMs);
    int getSendTimeout() const;

    ProducerConfiguration& setCompressionType(CompressionType compressionType);
    CompressionType getCompressionType() const;

    ProducerConfiguration& setMaxPendingMessages(int maxPendingMessages);
    int getMaxPendingMessages() const;

    ProducerConfiguration& setPartitionsRoutingMode(const PartitionsRoutingMode& mode);
    PartitionsRoutingMode getPartitionsRoutingMode() const;

    ProducerConfiguration& setMessageRouter(const MessageRoutingPolicyPtr& router);
    const MessageRoutingPolicyPtr& getMessageRouterPtr() const;

    ProducerConfiguration& setBlockIfQueueFull(bool);
    bool getBlockIfQueueFull() const;

    // Zero queue size feature will not be supported on consumer end if batching is enabled
    ProducerConfiguration& setBatchingEnabled(const bool& batchingEnabled);
    const bool& getBatchingEnabled() const;

    ProducerConfiguration& setBatchingMaxMessages(const unsigned int& batchingMaxMessages);
    const unsigned int& getBatchingMaxMessages() const;

    ProducerConfiguration& setBatchingMaxAllowedSizeInBytes(const unsigned long& batchingMaxAllowedSizeInBytes);
    const unsigned long& getBatchingMaxAllowedSizeInBytes() const;

    ProducerConfiguration& setBatchingMaxPublishDelayMs(const unsigned long& batchingMaxPublishDelayMs);
    const unsigned long& getBatchingMaxPublishDelayMs() const;
 private:
    struct Impl;
    boost::shared_ptr<Impl> impl_;
};

class ProducerImplBase;

class Producer {
 public:
    /**
     * Construct an uninitialized Producer.
     */
    Producer();

    /**
     * @return the topic to which producer is publishing to
     */
    const std::string& getTopic() const;

    /**
     * Publish a message on the topic associated with this Producer.
     *
     * This method will block until the message will be accepted and persisted
     * by the broker. In case of errors, the client library will try to
     * automatically recover and use a different broker.
     *
     * If it wasn't possible to successfully publish the message within the sendTimeout,
     * an error will be returned.
     *
     * This method is equivalent to asyncSend() and wait until the callback is triggered.
     *
     * @param msg message to publish
     * @return ResultOk if the message was published successfully
     * @return ResultWriteError if it wasn't possible to publish the message
     */
    Result send(const Message& msg);

    /**
     * Asynchronously publish a message on the topic associated with this Producer.
     *
     * This method will initiate the publish operation and return immediately. The
     * provided callback will be triggered when the message has been be accepted and persisted
     * by the broker. In case of errors, the client library will try to
     * automatically recover and use a different broker.
     *
     * If it wasn't possible to successfully publish the message within the sendTimeout, the
     * callback will be triggered with a Result::WriteError code.
     *
     * @param msg message to publish
     * @param callback the callback to get notification of the completion
     */
    void sendAsync(const Message& msg, SendCallback callback);

    /**
     * Close the producer and release resources allocated.
     *
     * No more writes will be accepted from this producer. Waits until
     * all pending write requests are persisted. In case of errors,
     * pending writes will not be retried.
     *
     * @return an error code to indicate the success or failure
     */
    Result close();

    /**
     * Close the producer and release resources allocated.
     *
     * No more writes will be accepted from this producer. The provided callback will be
     * triggered when all pending write requests are persisted. In case of errors,
     * pending writes will not be retried.
     */
    void closeAsync(CloseCallback callback);

 private:
    typedef boost::shared_ptr<ProducerImplBase> ProducerImplBasePtr;
    explicit Producer(ProducerImplBasePtr);

    friend class ClientImpl;
    friend class PulsarFriend;

    ProducerImplBasePtr impl_;
};

}

#pragma GCC visibility pop

#endif /* PRODUCER_HPP_ */
