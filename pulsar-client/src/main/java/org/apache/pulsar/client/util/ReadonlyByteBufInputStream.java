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
package org.apache.pulsar.client.util;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wrap ByteBuf to InputStream, this will not move ByteBuf readerIndex.
 */
public final class ReadonlyByteBufInputStream extends InputStream {
    private final ByteBuf buffer;
    private final int endIndex;
    private int readerIndex;

    public ReadonlyByteBufInputStream(ByteBuf buffer) {
        this.buffer = buffer;
        this.readerIndex = buffer.readerIndex();
        this.endIndex = readerIndex + buffer.readableBytes();
    }

    @Override
    public int read() throws IOException {
        if (this.available() == 0) {
            return -1;
        }
        int b = this.buffer.getByte(readerIndex);
        readerIndex++;
        return b & 0xff;
    }

    @Override
    public int available() throws IOException {
        return this.endIndex - readerIndex;
    }
}
