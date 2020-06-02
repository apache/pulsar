// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// http://code.google.com/p/protobuf/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

/*
 * This file is derived from Google ProcolBuffer CodedOutputStream class
 * with adaptations to work directly with Netty ByteBuf instances.
 */

package org.apache.pulsar.common.util.protobuf;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.concurrent.FastThreadLocal;

import java.io.IOException;

import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;
import org.apache.pulsar.shaded.com.google.protobuf.v241.WireFormat;

@SuppressWarnings("checkstyle:JavadocType")
public class ByteBufCodedOutputStream {

    @SuppressWarnings("checkstyle:JavadocType")
    public interface ByteBufGeneratedMessage {
        int getSerializedSize();

        void writeTo(ByteBufCodedOutputStream output) throws IOException;
    }

    private ByteBuf buf;

    private final Handle<ByteBufCodedOutputStream> recyclerHandle;

    public static ByteBufCodedOutputStream get(ByteBuf buf) {
        ByteBufCodedOutputStream stream = RECYCLER.get();
        stream.buf = buf;
        return stream;
    }

    public void recycle() {
        buf = null;
        recyclerHandle.recycle(this);
    }

    private ByteBufCodedOutputStream(Handle<ByteBufCodedOutputStream> handle) {
        this.recyclerHandle = handle;
    }

    private static final Recycler<ByteBufCodedOutputStream> RECYCLER = new Recycler<ByteBufCodedOutputStream>() {
        protected ByteBufCodedOutputStream newObject(Recycler.Handle<ByteBufCodedOutputStream> handle) {
            return new ByteBufCodedOutputStream(handle);
        }
    };

    /** Write a single byte. */
    public void writeRawByte(final int value) {
        buf.writeByte(value);
    }

    /**
     * Encode and write a varint. {@code value} is treated as unsigned, so it won't be sign-extended if negative.
     */
    public void writeRawVarint32(int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                writeRawByte(value);
                return;
            } else {
                writeRawByte((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    /** Encode and write a tag. */
    public void writeTag(final int fieldNumber, final int wireType) throws IOException {
        writeRawVarint32(makeTag(fieldNumber, wireType));
    }

    /** Write an {@code int32} field, including tag, to the stream. */
    public void writeInt32(final int fieldNumber, final int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeInt32NoTag(value);
    }

    public void writeInt64(final int fieldNumber, final long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeInt64NoTag(value);
    }

    public void writeUInt64(final int fieldNumber, final long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeUInt64NoTag(value);
    }

    /** Write a {@code bool} field, including tag, to the stream. */
    public void writeBool(final int fieldNumber, final boolean value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeBoolNoTag(value);
    }

    /** Write a {@code bool} field to the stream. */
    public void writeBoolNoTag(final boolean value) throws IOException {
      writeRawByte(value ? 1 : 0);
    }

    /** Write a {@code uint64} field to the stream. */
    public void writeInt64NoTag(final long value) throws IOException {
        writeRawVarint64(value);
    }

    /** Write a {@code uint64} field to the stream. */
    public void writeUInt64NoTag(final long value) throws IOException {
        writeRawVarint64(value);
    }

    /** Encode and write a varint. */
    public void writeRawVarint64(long value) throws IOException {
        while (true) {
            if ((value & ~0x7FL) == 0) {
                writeRawByte((int) value);
                return;
            } else {
                writeRawByte(((int) value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    /** Write a {@code bytes} field, including tag, to the stream. */
    public void writeBytes(final int fieldNumber, final ByteString value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        writeBytesNoTag(value);
    }

    /** Write a {@code bytes} field to the stream. */
    public void writeBytesNoTag(final ByteString value) throws IOException {
        writeRawVarint32(value.size());
        writeRawBytes(value);
    }


    private static final FastThreadLocal<byte[]> localByteArray = new FastThreadLocal<>();

    /** Write a byte string. */
    public void writeRawBytes(final ByteString value) throws IOException {
        byte[] localBuf = localByteArray.get();
        if (localBuf == null || localBuf.length < value.size()) {
            localBuf = new byte[Math.max(value.size(), 1024)];
            localByteArray.set(localBuf);
        }

        value.copyTo(localBuf, 0);
        buf.writeBytes(localBuf, 0, value.size());
    }

    public void writeEnum(final int fieldNumber, final int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeEnumNoTag(value);
    }

    /** Write a {@code uint32} field, including tag, to the stream. */
    public void writeUInt32(final int fieldNumber, final int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeUInt32NoTag(value);
    }

    /** Write a {@code uint32} field to the stream. */
    public void writeUInt32NoTag(final int value) throws IOException {
        writeRawVarint32(value);
    }

    public void writeSFixed64(final int fieldNumber, long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED64);
        writeSFixed64NoTag(value);
    }

    public void writeSFixed64NoTag(long value) throws IOException {
        buf.writeLongLE(value);
    }

    /**
     * Write an enum field to the stream. Caller is responsible for converting the enum value to its numeric value.
     */
    public void writeEnumNoTag(final int value) throws IOException {
        writeInt32NoTag(value);
    }

    /** Write an {@code int32} field to the stream. */
    public void writeInt32NoTag(final int value) throws IOException {
        if (value >= 0) {
            writeRawVarint32(value);
        } else {
            // Must sign-extend.
            writeRawVarint64(value);
        }
    }

    /** Write an embedded message field, including tag, to the stream. */
    public void writeMessage(final int fieldNumber, final ByteBufGeneratedMessage value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        writeMessageNoTag(value);
    }

    /** Write an embedded message field to the stream. */
    public void writeMessageNoTag(final ByteBufGeneratedMessage value) throws IOException {
        writeRawVarint32(value.getSerializedSize());
        value.writeTo(this);
    }

    static final int TAG_TYPE_BITS = 3;

    /** Makes a tag value given a field number and wire type. */
    static int makeTag(final int fieldNumber, final int wireType) {
        return (fieldNumber << TAG_TYPE_BITS) | wireType;
    }

    /** Write an double field, including tag, to the stream. */
    public void writeDouble(final int fieldNumber, double value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED64);
        buf.writeLongLE(Double.doubleToLongBits(value));
    }
}
