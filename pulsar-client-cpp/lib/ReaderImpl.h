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

#ifndef LIB_READERIMPL_H_
#define LIB_READERIMPL_H_

#include "ConsumerImpl.h"

namespace pulsar {

class ReaderImpl;

typedef std::shared_ptr<ReaderImpl> ReaderImplPtr;
typedef std::weak_ptr<ReaderImpl> ReaderImplWeakPtr;

class ReaderImpl : public std::enable_shared_from_this<ReaderImpl> {
   public:
    ReaderImpl(const ClientImplPtr client, const std::string& topic, const ReaderConfiguration& conf,
               const ExecutorServicePtr listenerExecutor, ReaderCallback readerCreatedCallback);

    void start(const MessageId& startMessageId);

    const std::string& getTopic() const;

    Result readNext(Message& msg);
    Result readNext(Message& msg, int timeoutMs);

    void closeAsync(ResultCallback callback);

    Future<Result, ReaderImplWeakPtr> getReaderCreatedFuture();

    ConsumerImplPtr getConsumer();

    void hasMessageAvailableAsync(HasMessageAvailableCallback callback);

   private:
    void handleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumer);

    void messageListener(Consumer consumer, const Message& msg);

    void acknowledgeIfNecessary(Result result, const Message& msg);

    std::string topic_;
    ClientImplWeakPtr client_;
    ReaderConfiguration readerConf_;
    ConsumerImplPtr consumer_;
    ReaderCallback readerCreatedCallback_;
    ReaderListener readerListener_;
};
}  // namespace pulsar

#endif /* LIB_READERIMPL_H_ */
