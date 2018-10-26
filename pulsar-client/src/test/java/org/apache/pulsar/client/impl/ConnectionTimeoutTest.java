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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionTimeoutTest {

    // 192.0.2.0/24 is assigned for documentation, should be a deadend
    final static String blackholeBroker = "pulsar://192.0.2.1:1234";

    @Test
    public void testLowTimeout() throws Exception {
        long startNanos = System.nanoTime();
        try (PulsarClient client = PulsarClient.builder().serviceUrl(blackholeBroker)
                .connectionTimeout(1, TimeUnit.MILLISECONDS).build()) {
            client.newProducer().topic("foo").create();
            Assert.fail("Shouldn't be able to connect to anything");
        } catch (PulsarClientException pse) {
            Assert.assertEquals(pse.getCause().getCause().getClass(), ConnectTimeoutException.class);
            Assert.assertTrue((System.nanoTime() - startNanos) < TimeUnit.SECONDS.toNanos(3));
        }
    }

    @Test
    public void testHighTimeout() throws Exception {
        try (PulsarClient client = PulsarClient.builder().serviceUrl(blackholeBroker)
                .connectionTimeout(24, TimeUnit.HOURS).build()) {
            CompletableFuture<Producer<byte[]>> f = client.newProducer().topic("foo").createAsync();

            Thread.sleep(12000); // sleep for 12 seconds (default timeout is 10)

            Assert.assertFalse(f.isDone());
        }
    }
}
