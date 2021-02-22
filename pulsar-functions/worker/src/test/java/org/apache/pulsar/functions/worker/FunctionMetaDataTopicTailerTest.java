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
package org.apache.pulsar.functions.worker;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionMetaDataTopicTailer}.
 */
@Slf4j
public class FunctionMetaDataTopicTailerTest {

    private static final String TEST_NAME = "test-fmt";

    private final Reader reader;
    private final ReaderBuilder readerBuilder;
    private final FunctionMetaDataManager fsm;
    private final FunctionMetaDataTopicTailer fsc;

    public FunctionMetaDataTopicTailerTest() throws Exception {
        this.reader = mock(Reader.class);
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
    public void testUpdate() throws Exception {

        FunctionMetaData request = FunctionMetaData.newBuilder().build();

        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(request.toByteArray());
        CountDownLatch readLatch = new CountDownLatch(1);
        CountDownLatch processLatch = new CountDownLatch(1);
        when(reader.readNext(anyInt(), any(TimeUnit.class))).thenReturn(msg).then(new Answer<Message>() {
            public Message answer(InvocationOnMock invocation) {
                try {
                    readLatch.countDown();
                    processLatch.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });

        fsc.start();

        readLatch.await();

        verify(reader, times(2)).readNext(anyInt(), any(TimeUnit.class));
        verify(fsm, times(1)).processMetaDataTopicMessage(any(Message.class));
    }
}
