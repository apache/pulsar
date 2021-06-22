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
#include <functional>
#include <memory>

#include <boost/asio.hpp>

namespace pulsar {

/**
 * A task that is executed periodically.
 *
 * After the `start()` method is called, it will trigger `callback_` method periodically whose interval is
 * `periodMs` in the constructor. After the `stop()` method is called, the timer will be cancelled and
 * `callback()` will never be called again unless `start()` was called again.
 *
 * If you don't want to execute the task infinitely, you can call `stop()` in the implementation of
 * `callback()` method.
 *
 * NOTE: If the `periodMs` is negative, the `callback()` will never be called.
 */
class PeriodicTask : public std::enable_shared_from_this<PeriodicTask> {
   public:
    using ErrorCode = boost::system::error_code;
    using CallbackType = std::function<void(const ErrorCode&)>;

    enum State : std::uint8_t
    {
        Pending,
        Ready,
        Closing
    };

    PeriodicTask(boost::asio::io_service& ioService, int periodMs) : timer_(ioService), periodMs_(periodMs) {}

    void start();

    void stop();

    void setCallback(CallbackType callback) noexcept { callback_ = callback; }

    State getState() const noexcept { return state_; }
    int getPeriodMs() const noexcept { return periodMs_; }

   private:
    std::atomic<State> state_{Pending};
    boost::asio::deadline_timer timer_;
    const int periodMs_;
    CallbackType callback_{trivialCallback};

    void handleTimeout(const ErrorCode& ec);

    static void trivialCallback(const ErrorCode&) {}
};

}  // namespace pulsar
