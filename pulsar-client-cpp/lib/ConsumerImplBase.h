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
#ifndef PULSAR_CONSUMER_IMPL_BASE_HEADER
#define PULSAR_CONSUMER_IMPL_BASE_HEADER
#include <pulsar/Message.h>
#include <pulsar/Consumer.h>

#include <set>

namespace pulsar {
class ConsumerImplBase;

typedef std::weak_ptr<ConsumerImplBase> ConsumerImplBaseWeakPtr;

class ConsumerImplBase {
   public:
    virtual ~ConsumerImplBase() {}
    virtual Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture() = 0;
    virtual const std::string& getSubscriptionName() const = 0;
    virtual const std::string& getTopic() const = 0;
    virtual Result receive(Message& msg) = 0;
    virtual Result receive(Message& msg, int timeout) = 0;
    virtual void receiveAsync(ReceiveCallback& callback) = 0;
    virtual void unsubscribeAsync(ResultCallback callback) = 0;
    virtual void acknowledgeAsync(const MessageId& msgId, ResultCallback callback) = 0;
    virtual void acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) = 0;
    virtual void closeAsync(ResultCallback callback) = 0;
    virtual void start() = 0;
    virtual void shutdown() = 0;
    virtual bool isClosed() = 0;
    virtual bool isOpen() = 0;
    virtual Result pauseMessageListener() = 0;
    virtual Result resumeMessageListener() = 0;
    virtual void redeliverUnacknowledgedMessages() = 0;
    virtual const std::string& getName() const = 0;
    virtual int getNumOfPrefetchedMessages() const = 0;
    virtual void getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) = 0;
    virtual void seekAsync(const MessageId& msgId, ResultCallback callback) = 0;
    virtual void negativeAcknowledge(const MessageId& msgId) = 0;
};
}  // namespace pulsar
#endif  // PULSAR_CONSUMER_IMPL_BASE_HEADER
