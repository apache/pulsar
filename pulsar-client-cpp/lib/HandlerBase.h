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
#ifndef _PULSAR_HANDLER_BASE_HEADER_
#define _PULSAR_HANDLER_BASE_HEADER_
#include "Backoff.h"
#include "ClientImpl.h"
#include "ClientConnection.h"
#include <memory>
#include <boost/asio.hpp>
#include <string>
#include <boost/date_time/local_time/local_time.hpp>

namespace pulsar {

using namespace boost::posix_time;
using boost::posix_time::milliseconds;
using boost::posix_time::seconds;

class HandlerBase;
typedef std::weak_ptr<HandlerBase> HandlerBaseWeakPtr;
typedef std::shared_ptr<HandlerBase> HandlerBasePtr;

class HandlerBase {
   public:
    HandlerBase(const ClientImplPtr&, const std::string&, const Backoff&);

    virtual ~HandlerBase();

    void start();

    /*
     * get method for derived class to access weak ptr to connection so that they
     * have to check if they can get a shared_ptr out of it or not
     */
    ClientConnectionWeakPtr getCnx() const { return connection_; }

   protected:
    /*
     * tries reconnection and sets connection_ to valid object
     */
    void grabCnx();

    /*
     * Schedule reconnection after backoff time
     */
    static void scheduleReconnection(HandlerBasePtr handler);

    /*
     * Should we retry in error that are transient
     */
    bool isRetriableError(Result result);
    /*
     * connectionOpened will be implemented by derived class to receive notification
     */

    virtual void connectionOpened(const ClientConnectionPtr& connection) = 0;

    virtual void connectionFailed(Result result) = 0;

    virtual HandlerBaseWeakPtr get_weak_from_this() = 0;

    virtual const std::string& getName() const = 0;

   private:
    static void handleNewConnection(Result result, ClientConnectionWeakPtr connection, HandlerBaseWeakPtr wp);
    static void handleDisconnection(Result result, ClientConnectionWeakPtr connection, HandlerBaseWeakPtr wp);

    static void handleTimeout(const boost::system::error_code& ec, HandlerBasePtr handler);

   protected:
    ClientImplWeakPtr client_;
    const std::string topic_;
    ClientConnectionWeakPtr connection_;
    ExecutorServicePtr executor_;
    mutable std::mutex mutex_;
    std::mutex pendingReceiveMutex_;
    ptime creationTimestamp_;

    const TimeDuration operationTimeut_;
    typedef std::unique_lock<std::mutex> Lock;

    enum State
    {
        NotStarted,
        Pending,
        Ready,
        Closing,
        Closed,
        Failed
    };

    std::atomic<State> state_;
    Backoff backoff_;
    uint64_t epoch_;

   private:
    DeadlineTimerPtr timer_;
    friend class ClientConnection;
    friend class PulsarFriend;
};
}  // namespace pulsar
#endif  //_PULSAR_HANDLER_BASE_HEADER_
