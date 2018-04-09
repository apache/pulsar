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
package org.apache.pulsar.tests;

import java.util.concurrent.TimeUnit;

import org.arquillian.cube.spi.Cube;
import org.arquillian.cube.spi.await.AwaitStrategy;
import org.arquillian.cube.docker.impl.client.config.Await;
import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.docker.impl.util.Ping;
import org.arquillian.cube.docker.impl.util.PingCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperAwaitStrategy implements AwaitStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperAwaitStrategy.class);

    private static final int DEFAULT_POLL_ITERATIONS = 10;
    private static final int DEFAULT_SLEEP_TIME = 1;
    private static final TimeUnit DEFAULT_SLEEP_TIMEUNIT = TimeUnit.SECONDS;

    private Cube<?> cube;
    private DockerClientExecutor dockerClientExecutor;

    @Override
    public boolean await() {
        return Ping.ping(DEFAULT_POLL_ITERATIONS, DEFAULT_SLEEP_TIME, DEFAULT_SLEEP_TIMEUNIT,
                new PingCommand() {
                    @Override
                    public boolean call() {
                        return PulsarClusterUtils.zookeeperRunning(dockerClientExecutor.getDockerClient(),
                                                                   cube.getId());
                    }
                });
    }
}
