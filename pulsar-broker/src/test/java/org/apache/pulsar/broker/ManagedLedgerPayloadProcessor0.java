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
package org.apache.pulsar.broker;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;

@SuppressWarnings("unused") // Used by PublishWithMLPayloadProcessorTest
public class ManagedLedgerPayloadProcessor0 implements ManagedLedgerPayloadProcessor {

    private final AtomicInteger counter = new AtomicInteger(0);
    private final int failAt = 4;

    @Override
    public Processor inputProcessor() {
        return new Processor() {
            @Override
            public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                if (counter.incrementAndGet() == failAt) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("Failed to process input payload");
                }
                return inputPayload.retainedDuplicate();
            }

            @Override
            public void release(ByteBuf processedPayload) {
                processedPayload.release();
            }
        };
    }
}
