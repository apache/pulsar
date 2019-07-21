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
#ifndef _PULSAR_EXECUTOR_SERVICE_HEADER_
#define _PULSAR_EXECUTOR_SERVICE_HEADER_

#include <memory>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <functional>
#include <thread>
#include <boost/noncopyable.hpp>
#include <mutex>
#include <pulsar/defines.h>

namespace pulsar {
typedef std::shared_ptr<boost::asio::ip::tcp::socket> SocketPtr;
typedef std::shared_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket &> > TlsSocketPtr;
typedef std::shared_ptr<boost::asio::ip::tcp::resolver> TcpResolverPtr;
typedef std::shared_ptr<boost::asio::deadline_timer> DeadlineTimerPtr;
class PULSAR_PUBLIC ExecutorService : private boost::noncopyable {
    friend class ClientConnection;

   public:
    ExecutorService();
    ~ExecutorService();

    SocketPtr createSocket();
    TlsSocketPtr createTlsSocket(SocketPtr &socket, boost::asio::ssl::context &ctx);
    TcpResolverPtr createTcpResolver();
    DeadlineTimerPtr createDeadlineTimer();
    void postWork(std::function<void(void)> task);
    void close();

   private:
    /*
     *  only called once and within lock so no need to worry about thread-safety
     */
    void startWorker();

    /*
     * io_service is our interface to os, io object schedule async ops on this object
     */
    boost::asio::io_service io_service_;

    /*
     * work will not let io_service.run() return even after it has finished work
     * it will keep it running in the background so we don't have to take care of it
     */
    typedef boost::asio::io_service::work BackgroundWork;
    std::unique_ptr<BackgroundWork> work_;

    /*
     * worker thread which runs until work object is destroyed, it's running io_service::run in
     * background invoking async handlers as they are finished and result is available from
     * io_service
     */
    boost::asio::detail::thread worker_;
};

typedef std::shared_ptr<ExecutorService> ExecutorServicePtr;

class PULSAR_PUBLIC ExecutorServiceProvider {
   public:
    explicit ExecutorServiceProvider(int nthreads);

    ExecutorServicePtr get();

    void close();

   private:
    typedef std::vector<ExecutorServicePtr> ExecutorList;
    ExecutorList executors_;
    int executorIdx_;
    std::mutex mutex_;
    typedef std::unique_lock<std::mutex> Lock;
};

typedef std::shared_ptr<ExecutorServiceProvider> ExecutorServiceProviderPtr;
}  // namespace pulsar

#endif  //_PULSAR_EXECUTOR_SERVICE_HEADER_
