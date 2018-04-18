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
    static final short BROKER_PORT = 8080;

    public static String zookeeperConnectString(DockerClient docker, String cluster) {
        return DockerUtils.cubeIdsWithLabels(docker, ImmutableMap.of("service", "zookeeper", "cluster", cluster))
            .stream().map((id) -> DockerUtils.getContainerIP(docker, id)).collect(Collectors.joining(":"));
    }

    public static ZooKeeper zookeeperClient(DockerClient docker, String cluster) throws Exception {
        String connectString = zookeeperConnectString(docker, cluster);
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

    public static boolean zookeeperRunning(DockerClient docker, String containerId) {
        String ip = DockerUtils.getContainerIP(docker, containerId);
        try (Socket socket = new Socket(ip, 2181)) {
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

    public static boolean waitZooKeeperUp(DockerClient docker, String cluster, int timeout, TimeUnit timeoutUnit)
            throws Exception {
        Optional<String> zookeeper = zookeeperSet(docker, cluster).stream().findAny();
        if (zookeeper.isPresent()) {
            long timeoutMillis = timeoutUnit.toMillis(timeout);
            long pollMillis = 1000;
            while (timeoutMillis > 0) {
                if (zookeeperRunning(docker, zookeeper.get())) {
                    return true;
                }
                Thread.sleep(pollMillis);
                timeoutMillis -= pollMillis;
            }
            return false;
        } else {
            LOG.warn("No zookeeper containers found");
            return false;
        }
    }

    public static boolean runOnAnyBroker(DockerClient docker, String cluster, String... cmds) throws Exception {
        Optional<String> broker = DockerUtils.cubeIdsWithLabels(
                docker,ImmutableMap.of("service", "pulsar-broker", "cluster", cluster)).stream().findAny();
        if (broker.isPresent()) {
            DockerUtils.runCommand(docker, broker.get(), cmds);
            return true;
        } else {
            return false;
        }
    }

    public static void runOnAllBrokers(DockerClient docker, String cluster, String... cmds) throws Exception {
        DockerUtils.cubeIdsWithLabels(docker,ImmutableMap.of("service", "pulsar-broker", "cluster", cluster))
            .stream().forEach((b) -> DockerUtils.runCommand(docker, b, cmds));
    }

    private static boolean waitBrokerState(DockerClient docker, String containerId,
                                           int timeout, TimeUnit timeoutUnit,
                                           boolean upOrDown) {
        long timeoutMillis = timeoutUnit.toMillis(timeout);
        long pollMillis = 1000;
        String brokerId = DockerUtils.getContainerHostname(docker, containerId) + ":" + BROKER_PORT;
        Optional<String> containerCluster = DockerUtils.getContainerCluster(docker, containerId);
        if (!containerCluster.isPresent()) {
            LOG.error("Unable to determine cluster for container {}. Missing label?", containerId);
            return false;
        }

        ZooKeeper zk = null;
        try {
            zk = zookeeperClient(docker, containerCluster.get());
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
                 containerId, upOrDown ? "up" : "down",
                 timeoutUnit.toSeconds(timeout));
        return false;
    }

    public static boolean waitBrokerUp(DockerClient docker, String containerId,
                                       int timeout, TimeUnit timeoutUnit) {
        if (waitBrokerState(docker, containerId, timeout, timeoutUnit, true)) {
            String ip = DockerUtils.getContainerIP(docker, containerId);

            long timeoutMillis = timeoutUnit.toMillis(timeout);
            long pollMillis = 100;

            while (timeoutMillis > 0) {
                try (Socket socket = new Socket(ip, BROKER_PORT)) {
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
        }
        return false;
    }

    public static boolean waitBrokerDown(DockerClient docker, String containerId,
                                         int timeout, TimeUnit timeoutUnit) {
        return waitBrokerState(docker, containerId, timeout, timeoutUnit, false);
    }

    public static boolean waitAllBrokersUp(DockerClient docker, String cluster) {
        return brokerSet(docker, cluster).stream()
            .map((b) -> waitBrokerUp(docker, b, 60, TimeUnit.SECONDS))
            .reduce(true, (accum, res) -> accum && res);
    }

    public static boolean waitAllBrokersDown(DockerClient docker, String cluster) {
        return brokerSet(docker, cluster).stream()
            .map((b) -> waitBrokerDown(docker, b, 60, TimeUnit.SECONDS))
            .reduce(true, (accum, res) -> accum && res);
    }

    public static boolean waitSupervisord(DockerClient docker, String containerId) {
        int i = 50;
        while (i > 0) {
            try {
                DockerUtils.runCommand(docker, containerId, "test", "-S", "/var/run/supervisor/supervisor.sock");
                return true;
            } catch (Exception e) {
                // supervisord not running
            }
            try {
                Thread.sleep(100);
                i++;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return false;
    }

    public static boolean startAllBrokers(DockerClient docker, String cluster) {
        brokerSet(docker, cluster).stream().forEach(
                (b) -> {
                    waitSupervisord(docker, b);
                    DockerUtils.runCommand(docker, b, "supervisorctl", "start", "broker");
                });

        return waitAllBrokersUp(docker, cluster);
    }

    public static boolean stopAllBrokers(DockerClient docker, String cluster) {
        brokerSet(docker, cluster).stream().forEach(
                (b) -> DockerUtils.runCommand(docker, b, "supervisorctl", "stop", "broker"));

        return waitAllBrokersDown(docker, cluster);
    }

    public static Set<String> brokerSet(DockerClient docker, String cluster) {
        return DockerUtils.cubeIdsWithLabels(docker, ImmutableMap.of("service", "pulsar-broker",
                                                                     "cluster", cluster));
    }

    public static boolean waitProxyUp(DockerClient docker, String containerId,
                                      int timeout, TimeUnit timeoutUnit) {
        String ip = DockerUtils.getContainerIP(docker, containerId);
        long timeoutMillis = timeoutUnit.toMillis(timeout);
        long pollMillis = 100;

        while (timeoutMillis > 0) {
            try (Socket socket = new Socket(ip, BROKER_PORT)) {
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

    public static boolean waitAllProxiesUp(DockerClient docker, String cluster) {
        return proxySet(docker, cluster).stream()
            .map((b) -> waitProxyUp(docker, b, 60, TimeUnit.SECONDS))
            .reduce(true, (accum, res) -> accum && res);
    }

    public static boolean startAllProxies(DockerClient docker, String cluster) {
        proxySet(docker, cluster).stream().forEach(
                (b) -> {
                    waitSupervisord(docker, b);
                    DockerUtils.runCommand(docker, b, "supervisorctl", "start", "proxy");
                });

        return waitAllProxiesUp(docker, cluster);
    }

    public static void stopAllProxies(DockerClient docker, String cluster) {
        proxySet(docker, cluster).stream().forEach(
                (b) -> DockerUtils.runCommand(docker, b, "supervisorctl", "stop", "proxy"));
    }

    public static Set<String> proxySet(DockerClient docker, String cluster) {
        return DockerUtils.cubeIdsWithLabels(docker, ImmutableMap.of("service", "pulsar-proxy",
                                                                     "cluster", cluster));
    }

    public static Set<String> zookeeperSet(DockerClient docker, String cluster) {
        return DockerUtils.cubeIdsWithLabels(docker, ImmutableMap.of("service", "zookeeper",
                                                                     "cluster", cluster));
    }
}
