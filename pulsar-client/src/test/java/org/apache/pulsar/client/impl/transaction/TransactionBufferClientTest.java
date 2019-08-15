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
package org.apache.pulsar.client.impl.transaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionHandler;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransactionBufferClientTest {

    PulsarClientImpl pulsarClient;
    ConnectionHandler connectionHandler;
    ClientCnx clientCnx;

    static class MockException extends Exception {
        public MockException(String msg) {
            super(msg);
        }
    }

    @BeforeMethod
    public void setup() {
        pulsarClient = mock(PulsarClientImpl.class);
        connectionHandler = mock(ConnectionHandler.class);
        clientCnx = mock(ClientCnx.class);
        ClientConfigurationData configurationData = mock(ClientConfigurationData.class);

        doReturn(configurationData).when(pulsarClient).getConfiguration();

        doReturn(clientCnx).when(connectionHandler).cnx();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return CompletableFuture.completedFuture(null);
            }
        }).when(clientCnx).sendTxnRequestWithId(any(), eq(1L));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return FutureUtil.failedFuture(new MockException("Failed"));
            }
        }).when(clientCnx).sendTxnRequestWithId(any(), eq(2L));

        doNothing().when(connectionHandler).grabCnx();

    }

    @Test
    public void testCommitSuccess() {
        String topic = "test-success-commit";
        TransactionBufferClientImpl client = new TransactionBufferClientImpl(pulsarClient, topic, connectionHandler);
        doReturn(1L).when(pulsarClient).newRequestId();
        try {
            client.commitTxnOnTopic(topic, 1, 1).get();
        } catch (Exception e) {
            fail("commit transaction should succeed");
        }
    }

    @Test
    public void testCommitFailed() {
        String topic = "test-fail-commit";
        TransactionBufferClientImpl client = new TransactionBufferClientImpl(pulsarClient, topic, connectionHandler);

        doReturn(2L).when(pulsarClient).newRequestId();
        try {
            client.commitTxnOnTopic(topic, 1, 1).get();
            fail("commit transaction should failed");
        } catch (Exception e) {
            // no-op
        }
    }

    @Test
    public void testAbortSuccess() {
        String topic = "test-success-abort";
        TransactionBufferClientImpl client = new TransactionBufferClientImpl(pulsarClient, topic, connectionHandler);

        doReturn(1L).when(pulsarClient).newRequestId();
        try {
            client.abortTxnOnTopic(topic, 1, 1).get();
        } catch (Exception e) {
            fail("abort transaction should succeed");
        }
    }

    @Test
    public void testAbortFailed() {
        String topic = "test-fail-abort";
        TransactionBufferClientImpl client = new TransactionBufferClientImpl(pulsarClient, topic, connectionHandler);

        doReturn(2L).when(pulsarClient).newRequestId();
        try {
            client.abortTxnOnTopic(topic, 1, 1).get();
            fail("abort transaction should failed");
        } catch (Exception e) {
            // no-op
        }
    }
}
