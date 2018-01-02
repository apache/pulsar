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
package org.apache.pulsar.functions.runtime.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.runtime.worker.request.DeregisterRequest;
import org.apache.pulsar.functions.runtime.worker.request.UpdateRequest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link FunctionStateConsumer}.
 */
@Slf4j
public class FunctionMetaDataConsumerTest {

    @Rule
    public final TestName runtime = new TestName();

    private final Consumer consumer;
    private final FunctionStateManager fsm;
    private final FunctionStateConsumer fsc;

    public FunctionMetaDataConsumerTest() throws Exception {
        this.consumer = mock(Consumer.class);
        this.fsm = mock(FunctionStateManager.class);
        this.fsc = new FunctionStateConsumer(fsm, consumer);
    }

    @After
    public void tearDown() throws Exception {
        fsc.close();
        verify(consumer, times(1)).close();
    }

    @Test
    public void testUpdate() throws Exception {
        UpdateRequest request = UpdateRequest.of(
            runtime.getMethodName(),
            mock(FunctionMetaData.class));

        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(Utils.toByteArray(request));

        CompletableFuture<Message> receiveFuture = CompletableFuture.completedFuture(msg);
        when(consumer.receiveAsync())
            .thenReturn(receiveFuture)
            .thenReturn(new CompletableFuture<>());

        fsc.start();

        // wait for receive future to complete
        receiveFuture.thenApply(Function.identity()).get();

        verify(consumer, times(2)).receiveAsync();
        verify(consumer, times(1)).acknowledgeAsync(eq(msg));
        verify(fsm, times(1)).processUpdate(any(UpdateRequest.class));
        verify(fsm, times(0)).proccessDeregister(any(DeregisterRequest.class));
    }

}
