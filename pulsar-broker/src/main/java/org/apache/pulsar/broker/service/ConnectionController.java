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
package org.apache.pulsar.broker.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.tls.InetAddressUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ConnectionController {

    /**
     * Increase the number of connections counter.
     * @param remoteAddress
     * @return
     */
    State increaseConnection(SocketAddress remoteAddress);

    /**
     * Decrease the number of connections counter.
     * @param remoteAddress
     */
    void decreaseConnection(SocketAddress remoteAddress);

    enum State {
        OK, REACH_MAX_CONNECTION_PER_IP, REACH_MAX_CONNECTION;
    }


    class DefaultConnectionController implements ConnectionController {
        private static final Logger log = LoggerFactory.getLogger(DefaultConnectionController.class);
        private static final Map<String, MutableInt> CONNECTIONS = new HashMap<>();
        private static final ReentrantLock lock = new ReentrantLock();
        private static int totalConnectionNum = 0;

        private final int maxConnections;
        private final int maxConnectionPerIp;
        private final boolean maxConnectionsLimitEnabled;
        private final boolean maxConnectionsLimitPerIpEnabled;

        public DefaultConnectionController(ServiceConfiguration configuration) {
            this.maxConnections = configuration.getBrokerMaxConnections();
            this.maxConnectionPerIp = configuration.getBrokerMaxConnectionsPerIp();
            this.maxConnectionsLimitEnabled = configuration.getBrokerMaxConnections() > 0;
            this.maxConnectionsLimitPerIpEnabled = configuration.getBrokerMaxConnectionsPerIp() > 0;
        }

        @Override
        public State increaseConnection(SocketAddress remoteAddress) {
            if (!maxConnectionsLimitEnabled && !maxConnectionsLimitPerIpEnabled) {
                return State.OK;
            }
            if (!(remoteAddress instanceof InetSocketAddress)
                    || !isLegalIpAddress(((InetSocketAddress) remoteAddress).getHostString())) {
                return State.OK;
            }
            lock.lock();
            try {
                String ip = ((InetSocketAddress) remoteAddress).getHostString();
                if (maxConnectionsLimitPerIpEnabled) {
                    CONNECTIONS.computeIfAbsent(ip, (x) -> new MutableInt(0)).increment();
                }
                if (maxConnectionsLimitEnabled) {
                    totalConnectionNum++;
                }
                if (maxConnectionsLimitEnabled && totalConnectionNum > maxConnections) {
                    log.info("Reject connect request from {}, because reached the maximum number of connections {}",
                            remoteAddress, totalConnectionNum);
                    return State.REACH_MAX_CONNECTION;
                }
                if (maxConnectionsLimitPerIpEnabled && CONNECTIONS.get(ip).getValue() > maxConnectionPerIp) {
                    log.info("Reject connect request from {}, because reached the maximum number "
                                    + "of connections per Ip {}",
                            remoteAddress, CONNECTIONS.get(ip).getValue());
                    return State.REACH_MAX_CONNECTION_PER_IP;
                }
            } catch (Exception e) {
                log.error("increase connection failed", e);
            } finally {
                lock.unlock();
            }
            return State.OK;
        }

        @Override
        public void decreaseConnection(SocketAddress remoteAddress) {
            if (!maxConnectionsLimitEnabled && !maxConnectionsLimitPerIpEnabled) {
                return;
            }
            if (!(remoteAddress instanceof InetSocketAddress)
                    || !isLegalIpAddress(((InetSocketAddress) remoteAddress).getHostString())) {
                return;
            }
            lock.lock();
            try {
                String ip = ((InetSocketAddress) remoteAddress).getHostString();
                MutableInt mutableInt = CONNECTIONS.get(ip);
                if (maxConnectionsLimitPerIpEnabled && mutableInt != null && mutableInt.decrementAndGet() <= 0) {
                    CONNECTIONS.remove(ip);
                }
                if (maxConnectionsLimitEnabled) {
                    totalConnectionNum--;
                }
            } finally {
                lock.unlock();
            }
        }

        private boolean isLegalIpAddress(String address) {
            return InetAddressUtils.isIPv4Address(address) || InetAddressUtils.isIPv6Address(address);
        }
    }


}
