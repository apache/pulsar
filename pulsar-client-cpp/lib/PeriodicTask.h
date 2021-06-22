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

#include <atomic>
#include <cstdint>
#include <memory>

#include <boost/asio.hpp>

namespace pulsar {

/**
 * A task that is executed periodically.
 */
class PeriodicTask : public std::enable_shared_from_this<PeriodicTask> {
   public:
    using ErrorCode = boost::system::error_code;

    enum State : std::uint8_t
    {
        Pending,
        Ready,
        Closing
    };

    PeriodicTask(boost::asio::io_service& ioService, int periodMs);

    void start();

    void stop();

    virtual void callback(const ErrorCode& ec) = 0;

    State getState() const noexcept { return state_; }

   private:
    std::atomic<State> state_{Pending};
    boost::asio::deadline_timer timer_;
    const int periodMs_;

    void handleTimeout(const ErrorCode& ec);
};

}  // namespace pulsar
