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
package org.apache.bookkeeper.mledger.offload.filesystem.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.bookkeeper.mledger.offload.filesystem.FileSystemReadHandleInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

public class FileSystemReadHandleInputStreamImpl extends FileSystemReadHandleInputStream {

    private final FSDataInputStream inputStream;
    private final int readBufferSize;
    private final ByteBuf byteBuf;
    private long readPos = 1;//the location to read
    private long remainLength;
    private long bufferOffsetStart;
    private long bufferOffsetEnd;
    private long objectLength;

    public FileSystemReadHandleInputStreamImpl(FSDataInputStream inputStream, int readBufferSize, long objectLength, long headerLength) {
        this.inputStream = inputStream;
        this.readBufferSize = readBufferSize;
        this.byteBuf = PooledByteBufAllocator.DEFAULT.buffer(readBufferSize, readBufferSize);
        this.objectLength = objectLength;
        this.remainLength = objectLength;
        this.readPos = headerLength;

    }

    private void needToFillBuff() throws IOException {
        byteBuf.clear();
        remainLength = (int)(objectLength - inputStream.getPos());
        bufferOffsetStart =  inputStream.getPos();
        byte[] bytes;
        if (remainLength > readBufferSize) {
            bytes = new byte[readBufferSize];
            inputStream.read(bytes);
        } else {
            bytes = new byte[(int)remainLength];
            inputStream.read(bytes);
        }
        bufferOffsetEnd = inputStream.getPos();
        byteBuf.writeBytes(bytes);
    }

    @Override
    public int read() throws IOException {
        if(readPos < objectLength) {
            if (readPos >= inputStream.getPos()) {
                needToFillBuff();
            }
            readPos++;
            return byteBuf.readUnsignedByte();
        }else {
            return -1;
        }
    }

    @Override
    public void seek(long length) throws IOException {
        if (length >= bufferOffsetStart && length <= bufferOffsetEnd) {
            long newIndex = length - bufferOffsetStart;
            readPos = bufferOffsetStart + newIndex;
            byteBuf.readerIndex((int)newIndex);
        } else {
            inputStream.seek(length);
            readPos = length;
            byteBuf.clear();
        }
    }

    @Override
    public void readByteBuf(ByteBuf entryBuf) throws IOException {
        long buffCanRead = bufferOffsetEnd - readPos;
        if(entryBuf.maxCapacity() > buffCanRead) {
            inputStream.seek(readPos);
            byte[] bytes = new byte[entryBuf.maxCapacity()];
            inputStream.read(bytes);
            entryBuf.writeBytes(bytes);
            readPos = inputStream.getPos();
            byteBuf.clear();
        } else {
            entryBuf.writeBytes(byteBuf.readBytes(entryBuf.capacity()));
            readPos += entryBuf.maxCapacity();
        }

    }

    @Override
    public void close() throws IOException {
        byteBuf.release();
        inputStream.close();
    }

    @Override
    public void skipLength(long length) throws IOException {
        seek(readPos + length);
    }
}
