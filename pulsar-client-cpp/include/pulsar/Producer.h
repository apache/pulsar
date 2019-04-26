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
#ifndef PRODUCER_HPP_
#define PRODUCER_HPP_

#include <pulsar/defines.h>
#include <pulsar/ProducerConfiguration.h>
#include <memory>
#include <stdint.h>

namespace pulsar {
class ProducerImplBase;
class PulsarWrapper;
class PulsarFriend;

typedef std::function<void(Result)> FlushCallback;
typedef std::shared_ptr<ProducerImplBase> ProducerImplBasePtr;

class PULSAR_PUBLIC Producer {
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
     * @return the producer name which could have been assigned by the system or specified by the client
     */
    const std::string& getProducerName() const;

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
     * Flush all the messages buffered in the client and wait until all messages have been successfully
     * persisted.
     */
    Result flush();

    /**
     * Flush all the messages buffered in the client and wait until all messages have been successfully
     * persisted.
     */
    void flushAsync(FlushCallback callback);

    /**
     * Get the last sequence id that was published by this producer.
     *
     * This represent either the automatically assigned or custom sequence id (set on the MessageBuilder) that
     * was published and acknowledged by the broker.
     *
     * After recreating a producer with the same producer name, this will return the last message that was
     * published in
     * the previous producer session, or -1 if there no message was ever published.
     *
     * @return the last sequence id published by this producer
     */
    int64_t getLastSequenceId() const;

    /**
     * Return an identifier for the schema version that this producer was created with.
     *
     * When the producer is created, if a schema info was passed, the broker will
     * determine the version of the passed schema. This identifier should be treated
     * as an opaque identifier. In particular, even though this is represented as a string, the
     * version might not be ascii printable.
     */
    const std::string& getSchemaVersion() const;

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
    explicit Producer(ProducerImplBasePtr);

    friend class ClientImpl;
    friend class PulsarFriend;
    friend class PulsarWrapper;

    ProducerImplBasePtr impl_;
};
}  // namespace pulsar

#endif /* PRODUCER_HPP_ */
