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

#pragma once

#include <pulsar/MessageId.h>

#include "ExecutorService.h"
#include "ClientImpl.h"

#include <mutex>
#include <map>

namespace pulsar {

class NegativeAcksTracker {
   public:
    NegativeAcksTracker(ClientImplPtr client, ConsumerImpl &consumer, const ConsumerConfiguration &conf);

    NegativeAcksTracker(const NegativeAcksTracker &) = delete;

    NegativeAcksTracker &operator=(const NegativeAcksTracker &) = delete;

    void add(const MessageId &m);

    void close();

   private:
    void scheduleTimer();
    void handleTimer(const boost::system::error_code &ec);

    static const std::chrono::milliseconds MIN_NACK_DELAY_NANOS;

    ConsumerImpl &consumer_;
    std::mutex mutex_;

    std::chrono::milliseconds nackDelay_;
    boost::posix_time::milliseconds timerInterval_;
    typedef typename std::chrono::steady_clock Clock;
    std::map<MessageId, Clock::time_point> nackedMessages_;

    ExecutorServicePtr executor_;
    DeadlineTimerPtr timer_;
};

}  // namespace pulsar
