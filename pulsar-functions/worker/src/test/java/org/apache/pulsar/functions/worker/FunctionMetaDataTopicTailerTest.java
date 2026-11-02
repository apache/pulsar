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
package org.apache.pulsar.functions.worker;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.awaitility.Awaitility;
import org.mockito.InOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionMetaDataTopicTailer}.
 */
@CustomLog
public class FunctionMetaDataTopicTailerTest {

    private Reader reader;
    private ReaderStub readerStub;
    private ReaderBuilder readerBuilder;
    private FunctionMetaDataManager fsm;
    private FunctionMetaDataTopicTailer fsc;

    @BeforeMethod(alwaysRun = true)
    @SuppressWarnings("unchecked")
    public void before() throws Exception {
        this.reader = mock(Reader.class);
        this.readerStub = new ReaderStub(reader);
        this.readerBuilder = mock(ReaderBuilder.class);
        when(readerBuilder.topic(anyString())).thenReturn(readerBuilder);
        when(readerBuilder.startMessageId(any(MessageId.class))).thenReturn(readerBuilder);
        when(readerBuilder.readerName(anyString())).thenReturn(readerBuilder);
        when(readerBuilder.subscriptionRolePrefix(anyString())).thenReturn(readerBuilder);
        when(readerBuilder.create()).thenReturn(reader);
        this.fsm = mock(FunctionMetaDataManager.class);
        this.fsc = new FunctionMetaDataTopicTailer(fsm, readerBuilder, new WorkerConfig(), MessageId.earliest,
                ErrorNotifier.getDefaultImpl());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        fsc.close();
        verify(reader, times(1)).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdate() throws Exception {

        FunctionMetaData request = new FunctionMetaData();

        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(request.toByteArray());
        readerStub.add(msg);

        fsc.start();

        // After processing the message, the tailer waits for the next one
        Awaitility.await().untilAsserted(() -> verify(reader, times(2)).readNextAsync());
        verify(fsm, times(1)).processMetaDataTopicMessage(any());
    }

    @Test(timeOut = 10000)
    @SuppressWarnings("unchecked")
    public void testStopWhenNoMoreMessages() throws Exception {
        Message msg1 = mock(Message.class);
        Message msg2 = mock(Message.class);
        readerStub.add(msg1);
        readerStub.add(msg2);

        fsc.start();

        // The tailer processes the messages left on the topic, then stops without waiting for another one
        fsc.stopWhenNoMoreMessages().get();
        InOrder inOrder = inOrder(fsm);
        inOrder.verify(fsm).processMetaDataTopicMessage(msg1);
        inOrder.verify(fsm).processMetaDataTopicMessage(msg2);
        verify(fsm, times(2)).processMetaDataTopicMessage(any());
    }

    @Test(timeOut = 10000)
    @SuppressWarnings("unchecked")
    public void testStopWhenNoMoreMessagesProcessesTheLastReceivedMessage() throws Exception {
        fsc.start();
        Awaitility.await().untilAsserted(() -> verify(reader).readNextAsync());

        // The reader takes the last message off the topic right before the tailer finds no more messages available
        Message msg = mock(Message.class);
        readerStub.receiveBeforeClose(msg);
        fsc.stopWhenNoMoreMessages().get();
        verify(fsm).processMetaDataTopicMessage(msg);
    }
}
