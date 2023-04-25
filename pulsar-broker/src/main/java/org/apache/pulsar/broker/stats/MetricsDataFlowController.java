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
package org.apache.pulsar.broker.stats;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class MetricsDataFlowController {

    private final ByteBuf buffer;
    private final int flowPermit;
    private final OutputStream output;
    private final TimeWindow<AtomicLong> window;

    public MetricsDataFlowController(ByteBuf buffer, OutputStream output, int flowPermit) {
        this.buffer = buffer;
        this.flowPermit = flowPermit;
        this.window = new TimeWindow<>(2, 1000);
        this.output = output;
    }

    private void recordAndFlush(int size) {
        long now = System.currentTimeMillis();
        WindowWrap<AtomicLong> wrap = this.window.current(__ -> new AtomicLong(0), now);
        @SuppressWarnings("all")
        long start = wrap.start();
        if (wrap.value().addAndGet(size) > this.flowPermit) {
            long nextWindowStart = start + window.interval();
            LockSupport.parkUntil(nextWindowStart);
            this.recordAndFlush(size);
        }
    }


    public void flushBuffer() throws IOException {
        int readIndex = this.buffer.readerIndex();
        int readableBytes = buffer.readableBytes();
        byte[] tmpBuf = new byte[512];
        int totalRead = 0;
        while (true) {
            int readable = Math.min(readableBytes - totalRead, 512);
            if (readable <= 0) {
                return;
            }
            buffer.getBytes(readIndex + totalRead, tmpBuf);
            this.recordAndFlush(readable);
            output.write(tmpBuf, 0, readable);
            totalRead += readable;
        }
    }

}
