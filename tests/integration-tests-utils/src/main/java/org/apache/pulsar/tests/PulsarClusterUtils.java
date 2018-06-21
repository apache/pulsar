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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.dockerjava.api.DockerClient;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.net.Socket;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarClusterUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarClusterUtils.class);
//    static final short BROKER_PORT = 8080;

    public static final String PULSAR_ADMIN = "/pulsar/bin/pulsar-admin";
    public static final String PULSAR = "/pulsar/bin/pulsar";
    public static final String PULSAR_CLIENT = "/pulsar/bin/pulsar-client";

    public static ZooKeeper zookeeperClient(String zkHost, String zkPort) throws Exception {
        String connectString = zkHost + ":" + zkPort;
        LOG.info("Connecting to zookeeper {}", connectString);
        CompletableFuture<Void> future = new CompletableFuture<>();
        ZooKeeper zk = new ZooKeeper(connectString, 10000,
            (e) -> {
                if (e.getState().equals(KeeperState.SyncConnected)) {
                    future.complete(null);
                }
            });
        future.get();
        return zk;
    }

    public static boolean zookeeperRunning(String zkHost, int zkPort) {
        try (Socket socket = new Socket(zkHost, zkPort)) {
            socket.setSoTimeout(1000);
            socket.getOutputStream().write("ruok".getBytes(UTF_8));
            byte[] resp = new byte[4];
            if (socket.getInputStream().read(resp) == 4) {
                return new String(resp, UTF_8).equals("imok");
            }
        } catch (IOException e) {
            // ignore, we'll return fallthrough to return false
        }
        return false;
    }

    public static boolean waitZooKeeperUp(String zkHost, int zkPort, int timeout, TimeUnit timeoutUnit)
        throws Exception {
            long timeoutMillis = timeoutUnit.toMillis(timeout);
            long pollMillis = 1000;
            while (timeoutMillis > 0) {
                if (zookeeperRunning(zkHost, zkPort)) {
                    return true;
                }
                Thread.sleep(pollMillis);
                timeoutMillis -= pollMillis;
            }
            return false;
    }

    private static boolean waitBrokerState(String brokerHost, String brokerPort, String zkHost, String zkPort,
                                           int timeout, TimeUnit timeoutUnit, boolean upOrDown) {
        long timeoutMillis = timeoutUnit.toMillis(timeout);
        long pollMillis = 1000;
        String brokerId = brokerHost + ":" + brokerPort;
        ZooKeeper zk = null;
        try {
            zk = zookeeperClient(zkHost, zkPort);
            String path = "/loadbalance/brokers/" + brokerId;
            while (timeoutMillis > 0) {
                if ((zk.exists(path, false) != null) == upOrDown) {
                    return true;
                }
                Thread.sleep(pollMillis);
                timeoutMillis -= pollMillis;
            }
        } catch (Exception e) {
            LOG.error("Exception checking for broker state", e);
            return false;
        } finally {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (Exception e) {
                LOG.error("Exception closing zookeeper client", e);
                return false;
            }
        }
        LOG.warn("Broker {} didn't go {} after {} seconds",
                 brokerHost, upOrDown ? "up" : "down",
                 timeoutUnit.toSeconds(timeout));
        return false;
    }


    public static boolean waitBrokerDown(String brokerHost, String brokerPort, String zkHost, String zkPort,
                                         int timeout, TimeUnit timeoutUnit) {
        return waitBrokerState(brokerHost, brokerPort, zkHost, zkPort, timeout, timeoutUnit, false);
    }


    public static void waitSupervisord(DockerClient docker, String containerId) {
        DockerUtils.runCommand(docker, containerId, "timeout", "60", "bash", "-c",
                               "until test -S /var/run/supervisor/supervisor.sock; do sleep 0.1; done");
    }


    public static boolean waitSocketAvailable(String containerIp, int port,
                                              int timeout, TimeUnit timeoutUnit) {
        long timeoutMillis = timeoutUnit.toMillis(timeout);
        long pollMillis = 100;

        while (timeoutMillis > 0) {
            try (Socket socket = new Socket(containerIp, port)) {
                return true;
            } catch (Exception e) {
                // couldn't connect, try again after sleep
            }
            try {
                Thread.sleep(pollMillis);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            timeoutMillis -= pollMillis;
        }
        return false;
    }

    public static void updateConf(DockerClient docker, String containerId,
                                  String confFile, String key, String value) throws Exception {
        String sedProgram = String.format(
                "/[[:blank:]]*%s[[:blank:]]*=/ { h; s^=.*^=%s^; }; ${x;/^$/ { s^^%s=%s^;H; }; x}",
                key, value, key, value);
        DockerUtils.runCommand(docker, containerId, "sed", "-i", "-e", sedProgram, confFile);
    }

    public static void setLogLevel(DockerClient docker, String containerId,
                                   String loggerName, String level) throws Exception {
        String sedProgram = String.format(
                "/  Logger:/ a\\\n"
                +"      - name: %s\\n"
                +"        level: %s\\n"
                +"        additivity: false\\n"
                +"        AppenderRef:\\n"
                +"          - ref: Console\\n"
                +"          - level: debug\\n",
                loggerName, level);
        String logConf = "/pulsar/conf/log4j2.yaml";
        DockerUtils.runCommand(docker, containerId, "sed", "-i", "-e", sedProgram, logConf);
    }
}
