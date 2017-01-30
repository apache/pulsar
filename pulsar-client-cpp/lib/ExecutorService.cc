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

#include "ExecutorService.h"

#include <boost/ref.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <boost/function.hpp>

namespace pulsar {

ExecutorService::ExecutorService()
        : io_service_(),
          work_(new BackgroundWork(io_service_)),
          worker_(boost::bind(&boost::asio::io_service::run, &io_service_)) {
}

ExecutorService::~ExecutorService() {
    close();
}

/*
 *  factory method of boost::asio::ip::tcp::socket associated with io_service_ instance
 *  @ returns shared_ptr to this socket
 */
SocketPtr ExecutorService::createSocket() {
    return boost::make_shared<boost::asio::ip::tcp::socket>(boost::ref(io_service_));
}

/*
 *  factory method of Resolver object associated with io_service_ instance
 *  @returns shraed_ptr to resolver object
 */
TcpResolverPtr ExecutorService::createTcpResolver() {
    return boost::make_shared<boost::asio::ip::tcp::resolver>(boost::ref(io_service_));
}

DeadlineTimerPtr ExecutorService::createDeadlineTimer() {
    return boost::make_shared<boost::asio::deadline_timer>(boost::ref(io_service_));
}

void ExecutorService::close() {
    io_service_.stop();
    work_.reset();
    worker_.join();
}

void ExecutorService::postWork(boost::function<void(void)> task) {
    io_service_.post(task);
}

/////////////////////

ExecutorServiceProvider::ExecutorServiceProvider(int nthreads)
        : executors_(nthreads),
          executorIdx_(0),
          mutex_() {
}

ExecutorServicePtr ExecutorServiceProvider::get() {
    Lock lock(mutex_);

    int idx = executorIdx_++ % executors_.size();
    if (!executors_[idx]) {
        executors_[idx] = boost::make_shared<ExecutorService>();
    }

    return executors_[idx];
}

void ExecutorServiceProvider::close() {
    for (ExecutorList::iterator it = executors_.begin(); it != executors_.end(); ++it) {
        if (*it != NULL) {
            (*it)->close();
        }
        it->reset();
    }
}

}
