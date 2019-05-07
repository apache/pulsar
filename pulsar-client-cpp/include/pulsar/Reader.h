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
#ifndef PULSAR_READER_HPP_
#define PULSAR_READER_HPP_

#include <pulsar/defines.h>
#include <pulsar/Message.h>
#include <pulsar/ReaderConfiguration.h>

namespace pulsar {
class PulsarWrapper;
class PulsarFriend;
class ReaderImpl;

typedef std::function<void(Result result, bool hasMessageAvailable)> HasMessageAvailableCallback;

/**
 * A Reader can be used to scan through all the messages currently available in a topic.
 */
class PULSAR_PUBLIC Reader {
   public:
    /**
     * Construct an uninitialized reader object
     */
    Reader();

    /**
     * @return the topic this reader is reading from
     */
    const std::string& getTopic() const;

    /**
     * Read a single message.
     *
     * If a message is not immediately available, this method will block until a new
     * message is available.
     *
     * @param msg a non-const reference where the received message will be copied
     * @return ResultOk when a message is received
     * @return ResultInvalidConfiguration if a message listener had been set in the configuration
     */
    Result readNext(Message& msg);

    /**
     * Read a single message
     *
     * @param msg a non-const reference where the received message will be copied
     * @param timeoutMs the receive timeout in milliseconds
     * @return ResultOk if a message was received
     * @return ResultTimeout if the receive timeout was triggered
     * @return ResultInvalidConfiguration if a message listener had been set in the configuration
     */
    Result readNext(Message& msg, int timeoutMs);

    Result close();

    void closeAsync(ResultCallback callback);

    /**
     * Asynchronously check if there is any message available to read from the current position.
     */
    void hasMessageAvailableAsync(HasMessageAvailableCallback callback);

    /**
     * Check if there is any message available to read from the current position.
     */
    Result hasMessageAvailable(bool& hasMessageAvailable);

   private:
    typedef std::shared_ptr<ReaderImpl> ReaderImplPtr;
    ReaderImplPtr impl_;
    explicit Reader(ReaderImplPtr);

    friend class PulsarFriend;
    friend class PulsarWrapper;
    friend class ReaderImpl;
    friend class ReaderTest;
};
}  // namespace pulsar

#endif /* PULSAR_READER_HPP_ */
