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
#ifndef _PULSAR_CONNECTION_POOL_HEADER_
#define _PULSAR_CONNECTION_POOL_HEADER_

#include <pulsar/defines.h>
#include <pulsar/Result.h>

#include "ClientConnection.h"

#include <atomic>
#include <string>
#include <map>
#include <mutex>
namespace pulsar {

class ExecutorService;

class PULSAR_PUBLIC ConnectionPool {
   public:
    ConnectionPool(const ClientConfiguration& conf, ExecutorServiceProviderPtr executorProvider,
                   const AuthenticationPtr& authentication, bool poolConnections = true);

    /**
     * Close the connection pool.
     *
     * @return false if it has already been closed.
     */
    bool close();

    /**
     * Get a connection from the pool.
     * <p>
     * The connection can either be created or be coming from the pool itself.
     * <p>
     * When specifying multiple addresses, the logicalAddress is used as a tag for the broker,
     * while the physicalAddress is where the connection is actually happening.
     * <p>
     * These two addresses can be different when the client is forced to connect through
     * a proxy layer. Essentially, the pool is using the logical address as a way to
     * decide whether to reuse a particular connection.
     *
     * @param logicalAddress the address to use as the broker tag
     * @param physicalAddress the real address where the TCP connection should be made
     * @return a future that will produce the ClientCnx object
     */
    Future<Result, ClientConnectionWeakPtr> getConnectionAsync(const std::string& logicalAddress,
                                                               const std::string& physicalAddress);

   private:
    ClientConfiguration clientConfiguration_;
    ExecutorServiceProviderPtr executorProvider_;
    AuthenticationPtr authentication_;
    typedef std::map<std::string, ClientConnectionWeakPtr> PoolMap;
    PoolMap pool_;
    bool poolConnections_;
    std::mutex mutex_;
    std::atomic_bool closed_{false};

    friend class ConnectionPoolTest;
};
}  // namespace pulsar
#endif  //_PULSAR_CONNECTION_POOL_HEADER_
