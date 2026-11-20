/*
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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.CustomLog;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.embedded.ExitHandler;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.assertj.core.util.Files;

@CustomLog
@SuppressWarnings("try")
public class TestZKServer implements AutoCloseable {

    public static final int TICK_TIME = 1000;

    private final File zkDataDir;
    // Clients connect to this port, which stays bound until close(), and reach the server through a proxy. A
    // restart can't bind the server's previous port again: while the server is stopped, that ephemeral-range port
    // is free for anyone, e.g. a Docker port mapping in another test fork, and the restart would then fail with
    // "Address already in use".
    private final ServerSocket proxyServerSocket;
    private final Set<Socket> proxiedSockets = ConcurrentHashMap.newKeySet();
    // The running server's own address, or null while it is stopped; guarded by this
    private InetSocketAddress serverAddress;
    private ZooKeeperServerEmbedded zooKeeperServerEmbedded;

    public TestZKServer() throws Exception {
        this.zkDataDir = Files.newTemporaryFolder();
        this.zkDataDir.deleteOnExit();
        // Allow all commands on ZK control port
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        // disable the admin server as to not have any port conflicts
        System.setProperty("zookeeper.admin.enableServer", "false");
        proxyServerSocket = new ServerSocket(0);
        Thread.ofVirtual().name("test-zk-proxy-acceptor").start(this::acceptConnections);
        start();
    }

    public void start() throws Exception {
        final Properties configZookeeper = new Properties();
        // Any free port: clients use the proxy port, which doesn't change
        configZookeeper.put("clientPort", "0");
        configZookeeper.put("host", "127.0.0.1");
        configZookeeper.put("ticktime", TICK_TIME + "");
        zooKeeperServerEmbedded = ZooKeeperServerEmbedded
                .builder()
                .baseDir(zkDataDir.toPath())
                .configuration(configZookeeper)
                .exitHandler(ExitHandler.LOG_ONLY)
                .build();

        zooKeeperServerEmbedded.start(60_000);

        ZooKeeperServerMain zooKeeperServerMain = getZooKeeperServerMain(zooKeeperServerEmbedded);
        int serverPort = getServerCnxnFactory(zooKeeperServerMain).getLocalPort();
        boolean zkServerReady = waitForServerUp("localhost:" + serverPort, 30_000);
        assertTrue(zkServerReady);
        synchronized (this) {
            serverAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), serverPort);
        }
        log.info().attr("connectionString", getConnectionString()).attr("serverPort", serverPort)
                .log("Started test ZK server");
    }

    private void acceptConnections() {
        while (true) {
            Socket client;
            try {
                client = proxyServerSocket.accept();
            } catch (IOException e) {
                // close() closed the proxy server socket
                return;
            }
            Socket server = connectToServer(client);
            if (server == null) {
                // The server is stopped: drop the connection, as the client then retries like on a refused one
                closeQuietly(client);
                continue;
            }
            Thread.ofVirtual().name("test-zk-proxy").start(() -> forward(client, server));
            Thread.ofVirtual().name("test-zk-proxy").start(() -> forward(server, client));
        }
    }

    // Registered under the lock that stop() takes, so no connection to a stopping server outlives stop()
    private synchronized Socket connectToServer(Socket client) {
        if (serverAddress == null) {
            return null;
        }
        Socket server = new Socket();
        try {
            client.setTcpNoDelay(true);
            server.setTcpNoDelay(true);
            server.connect(serverAddress);
        } catch (IOException e) {
            closeQuietly(server);
            return null;
        }
        proxiedSockets.add(client);
        proxiedSockets.add(server);
        return server;
    }

    private void forward(Socket from, Socket to) {
        try (from; to) {
            from.getInputStream().transferTo(to.getOutputStream());
        } catch (IOException e) {
            // Either side closed the connection, which the try-with-resources propagates to the other side
        } finally {
            proxiedSockets.remove(from);
            proxiedSockets.remove(to);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }

    @SneakyThrows
    private static ZooKeeperServerMain getZooKeeperServerMain(ZooKeeperServerEmbedded zooKeeperServerEmbedded) {
        ZooKeeperServerMain zooKeeperServerMain = readField(zooKeeperServerEmbedded.getClass(),
                "mainsingle", zooKeeperServerEmbedded);
        return zooKeeperServerMain;
    }

    @SneakyThrows
    private static ContainerManager getContainerManager(ZooKeeperServerMain zooKeeperServerMain) {
        ContainerManager containerManager = readField(ZooKeeperServerMain.class,
                "containerManager", zooKeeperServerMain);
        return containerManager;
    }

    @SneakyThrows
    private static ZooKeeperServer getZooKeeperServer(ZooKeeperServerMain zooKeeperServerMain) {
        ServerCnxnFactory serverCnxnFactory = getServerCnxnFactory(zooKeeperServerMain);
        ZooKeeperServer zkServer = readField(ServerCnxnFactory.class, "zkServer", serverCnxnFactory);
        return zkServer;
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @SneakyThrows
    private static <T> T readField(Class<?> clazz, String field, Object object) {
        Field declaredField = clazz.getDeclaredField(field);
        boolean accessible = declaredField.isAccessible();
        if (!accessible) {
            declaredField.setAccessible(true);
        }
        try {
            return (T) declaredField.get(object);
        } finally {
            declaredField.setAccessible(accessible);
        }
    }

    private static ServerCnxnFactory getServerCnxnFactory(ZooKeeperServerMain zooKeeperServerMain) throws Exception {
        ServerCnxnFactory serverCnxnFactory = readField(ZooKeeperServerMain.class, "cnxnFactory", zooKeeperServerMain);
        return serverCnxnFactory;
    }

    public void checkContainers() throws Exception {
        // Make sure the container nodes are actually deleted
        Thread.sleep(1000);

        ContainerManager containerManager = getContainerManager(getZooKeeperServerMain(zooKeeperServerEmbedded));
        containerManager.checkContainers();
    }

    public void stop() throws Exception {
        synchronized (this) {
            // Drop the clients' connections and refuse new ones, as the stopped server would
            serverAddress = null;
            proxiedSockets.forEach(TestZKServer::closeQuietly);
        }
        if (zooKeeperServerEmbedded != null) {
            zooKeeperServerEmbedded.close();
            zooKeeperServerEmbedded = null;
        }
        log.info("Stopped test ZK server");
    }

    public void expireSession(long sessionId) {
        getZooKeeperServer(getZooKeeperServerMain(zooKeeperServerEmbedded))
                .expire(sessionId);
    }

    public void setMultiReadLimits(int maxOps, long maxResponseBytes) {
        ZooKeeperServer zkServer = getZooKeeperServer(getZooKeeperServerMain(zooKeeperServerEmbedded));
        zkServer.setMultiReadMaxOps(maxOps);
        zkServer.setMultiReadMaxResponseBytes(maxResponseBytes);
    }

    @Override
    public void close() throws Exception {
        stop();
        proxyServerSocket.close();
        FileUtils.deleteDirectory(zkDataDir);
    }

    public String getConnectionString() {
        return "localhost:" + proxyServerSocket.getLocalPort();
    }

    public static boolean waitForServerUp(String hp, long timeout) {
        long start = System.currentTimeMillis();
        String[] split = hp.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (true) {
            try {
                Socket sock = new Socket(host, port);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        log.info("ZK Server UP");
                        return true;
                    }
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                // ignore as this is expected
                log.info().attr("server", hp).exceptionMessage(e).log("ZK server not up");
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }
}
