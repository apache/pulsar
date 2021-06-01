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
import java.util.regex.Pattern;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ConnectionController {

    /**
     * Increase the number of connections counter.
     * @param remoteAddress
     * @return
     */
    boolean increaseConnection(SocketAddress remoteAddress);

    /**
     * Decrease the number of connections counter.
     * @param remoteAddress
     */
    void decreaseConnection(SocketAddress remoteAddress);


    class DefaultConnectionController implements ConnectionController {
        private static final Logger log = LoggerFactory.getLogger(DefaultConnectionController.class);
        private final int maxConnections;
        private final int maxConnectionPerIp;

        private final static Map<String, MutableInt> CONNECTIONS = new HashMap<>();
        private static int totalConnectionNum = 0;
        private final static Pattern IPV4_PATTERN = Pattern
                .compile("^" + "(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)" + "(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}" + "$");

        private final static ReentrantLock lock = new ReentrantLock();

        public DefaultConnectionController(ServiceConfiguration configuration) {
            this.maxConnections = configuration.getBrokerMaxConnections();
            this.maxConnectionPerIp = configuration.getBrokerMaxConnectionsPerIp();
        }

        @Override
        public boolean increaseConnection(SocketAddress remoteAddress) {
            if (maxConnections <= 0 && maxConnectionPerIp <= 0) {
                return true;
            }
            if (!(remoteAddress instanceof InetSocketAddress) || !IPV4_PATTERN.matcher(
                    ((InetSocketAddress) remoteAddress).getHostString()).find()) {
                return true;
            }
            lock.lock();
            try {
                String ip = ((InetSocketAddress) remoteAddress).getHostString();
                CONNECTIONS.putIfAbsent(ip, new MutableInt(0));
                if (maxConnections > 0 && ++totalConnectionNum > maxConnections) {
                    log.info("Reject connect request from {}, because reached the maximum number of connections {}",
                            remoteAddress, totalConnectionNum);
                    return false;
                }
                if (maxConnectionPerIp > 0 && CONNECTIONS.get(ip).incrementAndGet() > maxConnectionPerIp) {
                    log.info("Reject connect request from {}, because reached the maximum number "
                                    + "of connections per Ip {}",
                            remoteAddress, CONNECTIONS.get(ip).getValue());
                    return false;
                }
            } catch (Exception e) {
                log.error("increase connection failed", e);
            } finally {
                lock.unlock();
            }
            return true;
        }

        @Override
        public void decreaseConnection(SocketAddress remoteAddress) {
            if (maxConnectionPerIp <= 0 && maxConnections <= 0) {
                return;
            }
            if (!(remoteAddress instanceof InetSocketAddress) || !IPV4_PATTERN.matcher(
                    ((InetSocketAddress) remoteAddress).getHostString()).find()) {
                return;
            }
            lock.lock();
            try {
                String ip = ((InetSocketAddress) remoteAddress).getHostString();
                MutableInt mutableInt = CONNECTIONS.get(ip);
                if (mutableInt == null) {
                    CONNECTIONS.remove(ip);
                    return;
                }
                if (maxConnectionPerIp > 0 && mutableInt.decrementAndGet() <= 0) {
                    CONNECTIONS.remove(ip);
                }
                if (maxConnections > 0) {
                    totalConnectionNum--;
                }
            } finally {
                lock.unlock();
            }
        }
    }


}
