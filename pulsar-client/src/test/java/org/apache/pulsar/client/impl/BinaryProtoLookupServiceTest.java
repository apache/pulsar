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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException.LookupException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BinaryProtoLookupServiceTest {
    private BinaryProtoLookupService lookup;
    private TopicName topicName;

    @BeforeMethod
    public void setup() throws Exception {
        LookupDataResult lookupResult1 = createLookupDataResult("pulsar://broker1.pulsar.apache.org:6650", true);
        LookupDataResult lookupResult2 = createLookupDataResult("pulsar://broker2.pulsar.apache.org:6650", false);

        CompletableFuture<LookupDataResult> lookupFuture1 = CompletableFuture.completedFuture(lookupResult1);
        CompletableFuture<LookupDataResult> lookupFuture2 = CompletableFuture.completedFuture(lookupResult2);

        ClientCnx clientCnx = mock(ClientCnx.class);
        when(clientCnx.newLookup(any(ByteBuf.class), anyLong())).thenReturn(lookupFuture1, lookupFuture1,
                lookupFuture2);

        CompletableFuture<ClientCnx> connectionFuture = CompletableFuture.completedFuture(clientCnx);

        ConnectionPool cnxPool = mock(ConnectionPool.class);
        when(cnxPool.getConnection(any(InetSocketAddress.class))).thenReturn(connectionFuture);

        ClientConfigurationData clientConfig = mock(ClientConfigurationData.class);
        doReturn(0).when(clientConfig).getMaxLookupRedirects();

        PulsarClientImpl client = mock(PulsarClientImpl.class);
        doReturn(cnxPool).when(client).getCnxPool();
        doReturn(clientConfig).when(client).getConfiguration();
        doReturn(1L).when(client).newRequestId();

        lookup = spy(
                new BinaryProtoLookupService(client, "pulsar://localhost:6650", false, mock(ExecutorService.class)));
        topicName = TopicName.get("persistent://tenant1/ns1/t1");
    }

    @Test(invocationTimeOut = 3000)
    public void maxLookupRedirectsTest1() throws Exception {
        Pair<InetSocketAddress, InetSocketAddress> addressPair = lookup.getBroker(topicName).get();
        assertEquals(addressPair.getLeft().toString(), "broker2.pulsar.apache.org:6650");
        assertEquals(addressPair.getRight().toString(), "broker2.pulsar.apache.org:6650");
    }

    @Test(invocationTimeOut = 3000)
    public void maxLookupRedirectsTest2() throws Exception {
        Field field = BinaryProtoLookupService.class.getDeclaredField("maxLookupRedirects");
        field.setAccessible(true);
        field.set(lookup, 2);

        Pair<InetSocketAddress, InetSocketAddress> addressPair = lookup.getBroker(topicName).get();
        assertEquals(addressPair.getLeft().toString(), "broker2.pulsar.apache.org:6650");
        assertEquals(addressPair.getRight().toString(), "broker2.pulsar.apache.org:6650");
    }

    @Test(invocationTimeOut = 3000)
    public void maxLookupRedirectsTest3() throws Exception {
        Field field = BinaryProtoLookupService.class.getDeclaredField("maxLookupRedirects");
        field.setAccessible(true);
        field.set(lookup, 1);

        try {
            lookup.getBroker(topicName).get();
            fail("should have thrown ExecutionException");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof LookupException);
            assertEquals(cause.getMessage(), "Too many redirects: 1");
        }
    }

    private static LookupDataResult createLookupDataResult(String brokerUrl, boolean redirect) throws Exception {
        LookupDataResult lookupResult = new LookupDataResult(-1);

        Field brokerUrlField = LookupDataResult.class.getDeclaredField("brokerUrl");
        brokerUrlField.setAccessible(true);
        brokerUrlField.set(lookupResult, brokerUrl);

        Field redirectField = LookupDataResult.class.getDeclaredField("redirect");
        redirectField.setAccessible(true);
        redirectField.set(lookupResult, redirect);

        return lookupResult;
    }
}
