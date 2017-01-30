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

#ifndef _PULSAR_EXECUTOR_SERVICE_HEADER_
#define _PULSAR_EXECUTOR_SERVICE_HEADER_

#include <boost/scoped_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>

#pragma GCC visibility push(default)

namespace pulsar {
typedef boost::shared_ptr<boost::asio::ip::tcp::socket> SocketPtr;
typedef boost::shared_ptr<boost::asio::ip::tcp::resolver> TcpResolverPtr;
typedef boost::shared_ptr<boost::asio::deadline_timer> DeadlineTimerPtr;
class ExecutorService : private boost::noncopyable {

 public:
    ExecutorService();
    ~ExecutorService();

    SocketPtr createSocket();
    TcpResolverPtr createTcpResolver();
    DeadlineTimerPtr createDeadlineTimer();
    void postWork(boost::function<void(void)> task);
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
    boost::scoped_ptr<BackgroundWork> work_;

    /*
     * worker thread which runs until work object is destroyed, it's running io_service::run in
     * background invoking async handlers as they are finished and result is available from
     * io_service
     */
    boost::asio::detail::thread worker_;
};

typedef boost::shared_ptr<ExecutorService> ExecutorServicePtr;

class ExecutorServiceProvider {
 public:
    explicit ExecutorServiceProvider(int nthreads);

    ExecutorServicePtr get();

    void close();

 private:
    typedef std::vector<ExecutorServicePtr> ExecutorList;
    ExecutorList executors_;
    int executorIdx_;
    boost::mutex mutex_;
    typedef boost::unique_lock<boost::mutex> Lock;
};

typedef boost::shared_ptr<ExecutorServiceProvider> ExecutorServiceProviderPtr;

}

#pragma GCC visibility pop

#endif //_PULSAR_EXECUTOR_SERVICE_HEADER_
