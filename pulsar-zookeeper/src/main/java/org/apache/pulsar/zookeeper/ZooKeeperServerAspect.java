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
package org.apache.pulsar.zookeeper;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import io.prometheus.client.Gauge;

/**
 * Instruments ZooKeeperServer to enable stats reporting on data set and z-node sizess
 */
@Aspect
public class ZooKeeperServerAspect {
    private static boolean metricsRegistered = false;

    @Pointcut("execution(org.apache.zookeeper.server.ZooKeeperServer.new(..))")
    public void zkServerConstructorPointCut() {
    }

    @After("zkServerConstructorPointCut()")
    public void zkServerConstructor(JoinPoint joinPoint) throws Throwable {
        // ZooKeeperServer instance was created
        ZooKeeperServer zkServer = (ZooKeeperServer) joinPoint.getThis();

        synchronized (ZooKeeperServerAspect.class) {
            if (metricsRegistered) {
                // We can only register the metrics a single time for the process
                return;
            }

            metricsRegistered = true;
        }

        Gauge.build().name("zookeeper_server_znode_count").help("Number of z-nodes stored").create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return zkServer.getZKDatabase().getNodeCount();
                    }
                }).register();

        Gauge.build().name("zookeeper_server_data_size_bytes").help("Size of all of z-nodes stored (bytes)").create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return zkServer.getZKDatabase().getDataTree().approximateDataSize();
                    }
                }).register();

        Gauge.build().name("zookeeper_server_connections").help("Number of currently opened connections").create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return zkServer.serverStats().getNumAliveClientConnections();
                    }
                }).register();

        Gauge.build().name("zookeeper_server_watches_count").help("Number of watches").create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return zkServer.getZKDatabase().getDataTree().getWatchCount();
                    }
                }).register();

        Gauge.build().name("zookeeper_server_ephemerals_count").help("Number of ephemerals z-nodes").create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return zkServer.getZKDatabase().getDataTree().getEphemeralsCount();
                    }
                }).register();
    }
}
