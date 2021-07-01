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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ConnectTimeoutException;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionTimeoutTest {

    // 192.0.2.0/24 is assigned for documentation, should be a deadend
    static final String blackholeBroker = "pulsar://192.0.2.1:1234";

    @Test
    public void testLowTimeout() throws Exception {
        long startNanos = System.nanoTime();

        try (PulsarClient clientLow = PulsarClient.builder().serviceUrl(blackholeBroker)
                .connectionTimeout(1, TimeUnit.MILLISECONDS)
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
             PulsarClient clientDefault = PulsarClient.builder().serviceUrl(blackholeBroker)
                 .operationTimeout(1000, TimeUnit.MILLISECONDS).build()) {
            CompletableFuture<?> lowFuture = clientLow.newProducer().topic("foo").createAsync();
            CompletableFuture<?> defaultFuture = clientDefault.newProducer().topic("foo").createAsync();

            try {
                lowFuture.get();
                Assert.fail("Shouldn't be able to connect to anything");
            } catch (Exception e) {
                Assert.assertFalse(defaultFuture.isDone());
                Assert.assertEquals(e.getCause().getCause().getCause().getClass(), ConnectTimeoutException.class);
            }
        }
    }
}
