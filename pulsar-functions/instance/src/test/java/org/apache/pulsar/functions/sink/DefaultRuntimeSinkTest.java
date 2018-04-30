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
package org.apache.pulsar.functions.sink;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.connect.core.RecordContext;
import org.apache.pulsar.connect.core.Sink;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link DefaultRuntimeSink}.
 */
public class DefaultRuntimeSinkTest {

    private Sink<String> mockSink;
    private RuntimeSink<String> runtimeSink;

    @BeforeMethod
    public void setup() {
        this.mockSink = mock(Sink.class);
        this.runtimeSink = DefaultRuntimeSink.of(mockSink);
    }

    @Test
    public void testOpen() throws Exception {
        this.runtimeSink.open(Collections.emptyMap());

        verify(mockSink, times(1)).open(any(Map.class));
    }

    @Test
    public void testClose() throws Exception {
        this.runtimeSink.close();

        verify(mockSink, times(1)).close();
    }

    @Test
    public void testWrite() {
        this.runtimeSink.write("test-record");
        verify(mockSink, times(1)).write(eq("test-record"));
    }

    @Test
    public void testWriteAck() {
        RecordContext context = mock(RecordContext.class);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        writeFuture.complete(null);
        when(mockSink.write(anyString())).thenReturn(writeFuture);

        runtimeSink.write(context, "test-record");

        verify(context, times(1)).ack();
    }

    @Test
    public void testWriteFail() {
        RecordContext context = mock(RecordContext.class);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        writeFuture.completeExceptionally(new Exception("test-exception"));
        when(mockSink.write(anyString())).thenReturn(writeFuture);

        runtimeSink.write(context, "test-record");

        verify(context, times(1)).fail();
    }
}
