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
#include "ConnectionPool.h"

#include "LogUtils.h"
#include "Url.h"

#include <boost/iostreams/stream.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

using boost::asio::ip::tcp;
namespace ssl = boost::asio::ssl;
typedef ssl::stream<tcp::socket> ssl_socket;

DECLARE_LOG_OBJECT()

namespace pulsar {

ConnectionPool::ConnectionPool(const ClientConfiguration& conf, ExecutorServiceProviderPtr executorProvider,
                               const AuthenticationPtr& authentication, bool poolConnections)
    : clientConfiguration_(conf),
      executorProvider_(executorProvider),
      authentication_(authentication),
      pool_(),
      poolConnections_(poolConnections),
      mutex_() {}

Future<Result, ClientConnectionWeakPtr> ConnectionPool::getConnectionAsync(
    const std::string& logicalAddress, const std::string& physicalAddress) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (clientConfiguration_.isValidateHostName()) {
        // Create a context that uses the default paths for
        // finding CA certificates.
        ssl::context ctx(ssl::context::sslv23);
        ctx.set_default_verify_paths();

        // Open a socket and connect it to the remote host.
        boost::asio::io_service io_service;
        ssl_socket sock(io_service, ctx);
        tcp::resolver resolver(io_service);
        Url service_url;
        Url::parse(physicalAddress, service_url);
        LOG_DEBUG("Validating hostname for " << service_url.host() << ":" << service_url.port());
        tcp::resolver::query query(service_url.host(), std::to_string(service_url.port()));
        boost::asio::connect(sock.lowest_layer(), resolver.resolve(query));
        sock.lowest_layer().set_option(tcp::no_delay(true));

        // Perform SSL handshake and verify the remote host's
        // certificate.
        sock.set_verify_mode(ssl::verify_peer);
        sock.set_verify_callback(ssl::rfc2818_verification(physicalAddress));
        sock.handshake(ssl_socket::client);
    }

    if (poolConnections_) {
        PoolMap::iterator cnxIt = pool_.find(logicalAddress);
        if (cnxIt != pool_.end()) {
            ClientConnectionPtr cnx = cnxIt->second.lock();

            if (cnx && !cnx->isClosed()) {
                // Found a valid or pending connection in the pool
                LOG_DEBUG("Got connection from pool for " << logicalAddress << " use_count: "  //
                                                          << (cnx.use_count() - 1) << " @ " << cnx.get());
                return cnx->getConnectFuture();
            } else {
                // Deleting stale connection
                LOG_INFO("Deleting stale connection from pool for "
                         << logicalAddress << " use_count: " << (cnx.use_count() - 1) << " @ " << cnx.get());
                pool_.erase(logicalAddress);
            }
        }
    }

    // No valid or pending connection found in the pool, creating a new one
    ClientConnectionPtr cnx(new ClientConnection(logicalAddress, physicalAddress, executorProvider_->get(),
                                                 clientConfiguration_, authentication_));

    LOG_INFO("Created connection for " << logicalAddress);

    Future<Result, ClientConnectionWeakPtr> future = cnx->getConnectFuture();
    pool_.insert(std::make_pair(logicalAddress, cnx));

    lock.unlock();

    cnx->tcpConnectAsync();
    return future;
}

}  // namespace pulsar
