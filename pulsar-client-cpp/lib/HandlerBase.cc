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
#include "HandlerBase.h"

#include <cassert>

#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

HandlerBase::HandlerBase(const ClientImplPtr& client, const std::string& topic, const Backoff& backoff)
    : client_(client),
      topic_(topic),
      connection_(),
      mutex_(),
      creationTimestamp_(now()),
      operationTimeut_(seconds(client->conf().getOperationTimeoutSeconds())),
      state_(Pending),
      backoff_(backoff),
      timer_(client->getIOExecutorProvider()->get()->createDeadlineTimer()) {}

HandlerBase::~HandlerBase() { timer_->cancel(); }

void HandlerBase::start() { grabCnx(); }

void HandlerBase::grabCnx() {
    Lock lock(mutex_);
    if (connection_.lock()) {
        lock.unlock();
        LOG_INFO(getName() << "Ignoring reconnection request since we're already connected");
        return;
    }
    lock.unlock();
    LOG_INFO(getName() << "Getting connection from pool");
    ClientImplPtr client = client_.lock();
    Future<Result, ClientConnectionWeakPtr> future = client->getConnection(topic_);
    future.addListener(std::bind(&HandlerBase::handleNewConnection, std::placeholders::_1,
                                 std::placeholders::_2, get_weak_from_this()));
}

void HandlerBase::handleNewConnection(Result result, ClientConnectionWeakPtr connection,
                                      HandlerBaseWeakPtr weakHandler) {
    HandlerBasePtr handler = weakHandler.lock();
    if (!handler) {
        LOG_DEBUG("HandlerBase Weak reference is not valid anymore");
        return;
    }
    if (result == ResultOk) {
        ClientConnectionPtr conn = connection.lock();
        if (conn) {
            LOG_DEBUG(handler->getName() << "Connected to broker: " << conn->cnxString());
            handler->connectionOpened(conn);
            return;
        }
        // TODO - look deeper into why the connection is null while the result is ResultOk
        LOG_INFO(handler->getName() << "ClientConnectionPtr is no longer valid");
    }
    handler->connectionFailed(result);
    scheduleReconnection(handler);
}

void HandlerBase::handleDisconnection(Result result, ClientConnectionWeakPtr connection,
                                      HandlerBaseWeakPtr weakHandler) {
    HandlerBasePtr handler = weakHandler.lock();
    if (!handler) {
        LOG_DEBUG("HandlerBase Weak reference is not valid anymore");
        return;
    }

    Lock lock(handler->mutex_);
    State state = handler->state_;

    ClientConnectionPtr currentConnection = handler->connection_.lock();
    if (currentConnection && connection.lock().get() != currentConnection.get()) {
        LOG_WARN(handler->getName()
                 << "Ignoring connection closed since we are already attached to a newer connection");
        return;
    }

    handler->connection_.reset();

    switch (state) {
        case Pending:
        case Ready:
            scheduleReconnection(handler);
            break;

        case Closing:
        case Closed:
        case Failed:
            LOG_DEBUG(handler->getName()
                      << "Ignoring connection closed event since the handler is not used anymore");
            break;
    }
}

bool HandlerBase::isRetriableError(Result result) {
    switch (result) {
        case ResultServiceUnitNotReady:
            return true;
        default:
            return false;
    }
}

void HandlerBase::scheduleReconnection(HandlerBasePtr handler) {
    if (handler->state_ == Pending || handler->state_ == Ready) {
        TimeDuration delay = handler->backoff_.next();

        LOG_INFO(handler->getName() << "Schedule reconnection in " << (delay.total_milliseconds() / 1000.0)
                                    << " s");
        handler->timer_->expires_from_now(delay);
        // passing shared_ptr here since time_ will get destroyed, so tasks will be cancelled
        // so we will not run into the case where grabCnx is invoked on out of scope handler
        handler->timer_->async_wait(std::bind(&HandlerBase::handleTimeout, std::placeholders::_1, handler));
    }
}

void HandlerBase::handleTimeout(const boost::system::error_code& ec, HandlerBasePtr handler) {
    if (ec) {
        LOG_DEBUG(handler->getName() << "Ignoring timer cancelled event, code[" << ec << "]");
        return;
    } else {
        handler->grabCnx();
    }
}

ptime now() { return microsec_clock::universal_time(); }

int64_t currentTimeMillis() {
    static ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));

    time_duration diff = now() - time_t_epoch;
    return diff.total_milliseconds();
}
}  // namespace pulsar
