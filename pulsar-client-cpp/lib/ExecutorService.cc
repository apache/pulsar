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
#include "ExecutorService.h"

#include <boost/asio.hpp>
#include <functional>
#include <memory>
#include "TimeUtils.h"

#include "LogUtils.h"
DECLARE_LOG_OBJECT()

namespace pulsar {

ExecutorService::ExecutorService() {}

ExecutorService::~ExecutorService() { close(0); }

void ExecutorService::start() {
    auto self = shared_from_this();
    std::thread t{[self] {
        if (self->isClosed()) {
            return;
        }
        LOG_INFO("Run io_service in a single thread");
        boost::system::error_code ec;
        self->getIOService().run(ec);
        if (ec) {
            LOG_ERROR("Failed to run io_service: " << ec.message());
        } else {
            LOG_INFO("Event loop of ExecutorService exits successfully");
        }
        self->ioServiceDone_ = true;
        self->cond_.notify_all();
    }};
    t.detach();
}

ExecutorServicePtr ExecutorService::create() {
    // make_shared cannot access the private constructor, so we need to expose the private constructor via a
    // derived class.
    struct ExecutorServiceImpl : public ExecutorService {};

    auto executor = std::make_shared<ExecutorServiceImpl>();
    executor->start();
    return std::static_pointer_cast<ExecutorService>(executor);
}

/*
 *  factory method of boost::asio::ip::tcp::socket associated with io_service_ instance
 *  @ returns shared_ptr to this socket
 */
SocketPtr ExecutorService::createSocket() { return SocketPtr(new boost::asio::ip::tcp::socket(io_service_)); }

TlsSocketPtr ExecutorService::createTlsSocket(SocketPtr &socket, boost::asio::ssl::context &ctx) {
    return std::shared_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket &> >(
        new boost::asio::ssl::stream<boost::asio::ip::tcp::socket &>(*socket, ctx));
}

/*
 *  factory method of Resolver object associated with io_service_ instance
 *  @returns shraed_ptr to resolver object
 */
TcpResolverPtr ExecutorService::createTcpResolver() {
    return TcpResolverPtr(new boost::asio::ip::tcp::resolver(io_service_));
}

DeadlineTimerPtr ExecutorService::createDeadlineTimer() {
    return DeadlineTimerPtr(new boost::asio::deadline_timer(io_service_));
}

void ExecutorService::close(long timeoutMs) {
    bool expectedState = false;
    if (!closed_.compare_exchange_strong(expectedState, true)) {
        return;
    }
    if (timeoutMs == 0) {  // non-blocking
        io_service_.stop();
        return;
    }

    std::unique_lock<std::mutex> lock{mutex_};
    io_service_.stop();
    if (timeoutMs > 0) {
        cond_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this] { return ioServiceDone_.load(); });
    } else {  // < 0
        cond_.wait(lock, [this] { return ioServiceDone_.load(); });
    }
}

void ExecutorService::postWork(std::function<void(void)> task) { io_service_.post(task); }

/////////////////////

ExecutorServiceProvider::ExecutorServiceProvider(int nthreads)
    : executors_(nthreads), executorIdx_(0), mutex_() {}

ExecutorServicePtr ExecutorServiceProvider::get() {
    Lock lock(mutex_);

    int idx = executorIdx_++ % executors_.size();
    if (!executors_[idx]) {
        executors_[idx] = ExecutorService::create();
    }

    return executors_[idx];
}

void ExecutorServiceProvider::close(long timeoutMs) {
    Lock lock(mutex_);

    TimeoutProcessor<std::chrono::milliseconds> timeoutProcessor{timeoutMs};
    for (auto &&executor : executors_) {
        timeoutProcessor.tik();
        if (executor) {
            executor->close(timeoutProcessor.getLeftTimeout());
        }
        timeoutProcessor.tok();
        executor.reset();
    }
}
}  // namespace pulsar
