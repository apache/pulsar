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
package org.apache.pulsar.client.impl;

import io.netty.channel.ConnectTimeoutException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionTimeoutTest {

    @Test
    public void testLowTimeout() throws Exception {
        int backlogSize = 1;
        // create a dummy server and fill the backlog of the server so that it won't respond
        // so that the client timeout can be tested with this server
        try (ServerSocket serverSocket = new ServerSocket(0, backlogSize, InetAddress.getByName("localhost"))) {
            CountDownLatch latch = new CountDownLatch(backlogSize + 1);
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < backlogSize + 1; i++) {
                Thread connectThread = new Thread(() -> {
                    try (Socket socket = new Socket()) {
                        socket.connect(serverSocket.getLocalSocketAddress());
                        latch.countDown();
                        Thread.sleep(10000L);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                connectThread.start();
                threads.add(connectThread);
            }
            latch.await();

            String blackholeBroker =
                    "pulsar://" + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();

            try (PulsarClient clientLow = PulsarClient.builder().serviceUrl(blackholeBroker)
                    .connectionTimeout(1, TimeUnit.MILLISECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build()) {
                CompletableFuture<?> lowFuture = clientLow.newProducer().topic("foo").createAsync();
                try {
                    lowFuture.get(10, TimeUnit.SECONDS);
                    Assert.fail("Shouldn't be able to connect to anything");
                } catch (TimeoutException e) {
                    Assert.fail("Connection timeout didn't apply.");
                } catch (Exception e) {
                    Assert.assertEquals(e.getCause().getCause().getCause().getClass(), ConnectTimeoutException.class);
                }
            } finally {
                threads.stream().forEach(Thread::interrupt);
            }
        }
    }
}
