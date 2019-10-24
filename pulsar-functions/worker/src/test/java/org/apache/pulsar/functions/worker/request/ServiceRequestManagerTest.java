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
package org.apache.pulsar.functions.worker.request;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.functions.proto.Request.ServiceRequest;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ServiceRequestManager}.
 */
@PrepareForTest(WorkerUtils.class)
public class ServiceRequestManagerTest {

    private final Producer producer;
    private final ServiceRequestManager reqMgr;

    public ServiceRequestManagerTest() throws Exception {
        this.producer = mock(Producer.class);
        this.reqMgr = new ServiceRequestManager(producer);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        reqMgr.close();
        verify(producer, times(1)).close();
    }

    @Test
    public void testSubmitRequest() throws Exception {
        ServiceRequest request = ServiceRequest.newBuilder().build();
        MessageId msgId = mock(MessageId.class);

        when(producer.sendAsync(any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(msgId));

        CompletableFuture<MessageId> submitFuture = reqMgr.submitRequest(request);
        assertSame(msgId, submitFuture.get());
        verify(producer, times(1)).sendAsync(any(byte[].class));
    }

}
