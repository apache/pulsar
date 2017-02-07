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

#ifndef _PULSAR_HANDLER_BASE_HEADER_
#define _PULSAR_HANDLER_BASE_HEADER_
#include "Backoff.h"
#include "ClientImpl.h"
#include "ClientConnection.h"
#include <boost/make_shared.hpp>
#include <boost/asio.hpp>
#include <string>
#include <boost/date_time/local_time/local_time.hpp>

namespace pulsar {

using namespace boost::posix_time;
using boost::posix_time::milliseconds;
using boost::posix_time::seconds;

ptime now();
int64_t currentTimeMillis();

class HandlerBase;
typedef boost::weak_ptr<HandlerBase> HandlerBaseWeakPtr;
typedef boost::shared_ptr<HandlerBase> HandlerBasePtr;

class HandlerBase {

 public:
    HandlerBase(const ClientImplPtr& client, const std::string& topic);

    virtual ~HandlerBase();

    void start();

    /*
     * get method for derived class to access weak ptr to connection so that they
     * have to check if they can get a shared_ptr out of it or not
     */
    ClientConnectionWeakPtr getCnx() {
        return connection_;
    }

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
    static void handleNewConnection(Result result, ClientConnectionWeakPtr connection,
                                    HandlerBaseWeakPtr wp);
    static void handleDisconnection(Result result, ClientConnectionWeakPtr connection,
                                    HandlerBaseWeakPtr wp);

    static void handleTimeout(const boost::system::error_code& ec, HandlerBasePtr handler);

 protected:
    ClientImplWeakPtr client_;
    const std::string topic_;
    ClientConnectionWeakPtr connection_;
    boost::mutex mutex_;
    ptime creationTimestamp_;

    const TimeDuration operationTimeut_;
    typedef boost::unique_lock<boost::mutex> Lock;

    enum State {
        Pending,
        Ready,
        Closing,
        Closed,
        Failed
    };

    State state_;
    Backoff backoff_;

 private:
    DeadlineTimerPtr timer_;
    friend class ClientConnection;
};
}
#endif  //_PULSAR_HANDLER_BASE_HEADER_
