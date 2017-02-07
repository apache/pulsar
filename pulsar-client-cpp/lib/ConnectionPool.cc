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

#include "ConnectionPool.h"

#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

ConnectionPool::ConnectionPool(const ClientConfiguration& conf,
                               ExecutorServiceProviderPtr executorProvider,
                               const AuthenticationPtr& authentication, bool poolConnections)
        : clientConfiguration_(conf),
          executorProvider_(executorProvider),
          authentication_(authentication),
          pool_(),
          poolConnections_(poolConnections),
          mutex_() {
}

Future<Result, ClientConnectionWeakPtr> ConnectionPool::getConnectionAsync(
        const std::string& endpoint) {
    boost::unique_lock<boost::mutex> lock(mutex_);

    if (poolConnections_) {
        PoolMap::iterator cnxIt = pool_.find(endpoint);
        if (cnxIt != pool_.end()) {
            ClientConnectionPtr cnx = cnxIt->second.lock();

            if (cnx && !cnx->isClosed()) {
                // Found a valid or pending connection in the pool
                LOG_DEBUG("Got connection from pool for " << endpoint << " use_count: "  //
                        << (cnx.use_count() - 1) << " @ " << cnx.get());
                return cnx->getConnectFuture();
            } else {
                // Deleting stale connection
                LOG_INFO("Deleting stale connection from pool for " << endpoint << " use_count: "
                        << (cnx.use_count() - 1) << " @ " << cnx.get());
                pool_.erase(endpoint);
            }
        }
    }

    // No valid or pending connection found in the pool, creating a new one
    ClientConnectionPtr cnx(new ClientConnection(endpoint, executorProvider_->get(), clientConfiguration_, authentication_));

    LOG_INFO("Created connection for " << endpoint);

    Future<Result, ClientConnectionWeakPtr> future = cnx->getConnectFuture();
    pool_.insert(std::make_pair(endpoint, cnx));

    lock.unlock();

    cnx->tcpConnectAsync();
    return future;
}

}
