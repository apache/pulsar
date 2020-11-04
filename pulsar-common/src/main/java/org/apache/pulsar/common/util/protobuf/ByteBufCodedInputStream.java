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
 * This file is derived from Google ProcolBuffer CodedInputStream class
 * with adaptations to work directly with Netty ByteBuf instances.
 */

package org.apache.pulsar.common.util.protobuf;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.concurrent.FastThreadLocal;

import java.io.IOException;

import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ExtensionRegistryLite;
import org.apache.pulsar.shaded.com.google.protobuf.v241.InvalidProtocolBufferException;
import org.apache.pulsar.shaded.com.google.protobuf.v241.WireFormat;

@SuppressWarnings("checkstyle:JavadocType")
public class ByteBufCodedInputStream {

    @SuppressWarnings("checkstyle:JavadocType")
    public interface ByteBufMessageBuilder {
        ByteBufMessageBuilder mergeFrom(ByteBufCodedInputStream input, ExtensionRegistryLite ext)
                throws java.io.IOException;
    }

    private ByteBuf buf;
    private int lastTag;

    private final Handle<ByteBufCodedInputStream> recyclerHandle;

    public static ByteBufCodedInputStream get(ByteBuf buf) {
        ByteBufCodedInputStream stream = RECYCLER.get();
        stream.buf = buf;
        stream.lastTag = 0;
        return stream;
    }

    private ByteBufCodedInputStream(Handle<ByteBufCodedInputStream> handle) {
        this.recyclerHandle = handle;
    }

    private static final Recycler<ByteBufCodedInputStream> RECYCLER = new Recycler<ByteBufCodedInputStream>() {
        protected ByteBufCodedInputStream newObject(Recycler.Handle<ByteBufCodedInputStream> handle) {
            return new ByteBufCodedInputStream(handle);
        }
    };

    public void recycle() {
        this.buf = null;
        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    public int readTag() throws IOException {
        if (isAtEnd()) {
            lastTag = 0;
            return 0;
        }

        lastTag = readRawVarint32();
        if (WireFormat.getTagFieldNumber(lastTag) == 0) {
            // If we actually read zero (or any tag number corresponding to field
            // number zero), that's not a valid tag.
            throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint.");
        }
        return lastTag;
    }

    /** Read a {@code uint32} field value from the stream. */
    public int readUInt32() throws IOException {
        return readRawVarint32();
    }

    /**
     * Read an enum field value from the stream. Caller is responsible for converting the numeric value to an actual
     * enum.
     */
    public int readEnum() throws IOException {
        return readRawVarint32();
    }

    public boolean isAtEnd() throws IOException {
        return !buf.isReadable();
    }

    /** Read an embedded message field value from the stream. */
    public void readMessage(final ByteBufMessageBuilder builder, final ExtensionRegistryLite extensionRegistry)
            throws IOException {
        final int length = readRawVarint32();

        int writerIdx = buf.writerIndex();
        buf.writerIndex(buf.readerIndex() + length);
        builder.mergeFrom(this, extensionRegistry);
        checkLastTagWas(0);
        buf.writerIndex(writerIdx);
    }

    private static final FastThreadLocal<byte[]> localByteArray = new FastThreadLocal<>();

    /** Read a {@code bytes} field value from the stream. */
    public ByteString readBytes() throws IOException {
        final int size = readRawVarint32();
        if (size == 0) {
            return ByteString.EMPTY;
        } else {
            byte[] localBuf = localByteArray.get();
            if (localBuf == null || localBuf.length < size) {
                localBuf = new byte[Math.max(size, 1024)];
                localByteArray.set(localBuf);
            }

            buf.readBytes(localBuf, 0, size);
            ByteString res = ByteString.copyFrom(localBuf, 0, size);
            return res;
        }
    }

    static final int TAG_TYPE_BITS = 3;
    static final int TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1;

    /** Given a tag value, determines the wire type (the lower 3 bits). */
    static int getTagWireType(final int tag) {
        return tag & TAG_TYPE_MASK;
    }

    /** Makes a tag value given a field number and wire type. */
    static int makeTag(final int fieldNumber, final int wireType) {
        return (fieldNumber << TAG_TYPE_BITS) | wireType;
    }

    /**
     * Reads and discards a single field, given its tag value.
     *
     * @return {@code false} if the tag is an endgroup tag, in which case nothing is skipped. Otherwise, returns
     *         {@code true}.
     */
    public boolean skipField(final int tag) throws IOException {
        switch (getTagWireType(tag)) {
        case WireFormat.WIRETYPE_VARINT:
            readInt32();
            return true;
        case WireFormat.WIRETYPE_FIXED64:
            readRawLittleEndian64();
            return true;
        case WireFormat.WIRETYPE_LENGTH_DELIMITED:
            skipRawBytes(readRawVarint32());
            return true;
        case WireFormat.WIRETYPE_START_GROUP:
            skipMessage();
            checkLastTagWas(makeTag(WireFormat.getTagFieldNumber(tag), WireFormat.WIRETYPE_END_GROUP));
            return true;
        case WireFormat.WIRETYPE_END_GROUP:
            return false;
        case WireFormat.WIRETYPE_FIXED32:
            readRawLittleEndian32();
            return true;
        default:
            throw new InvalidProtocolBufferException("Protocol message tag had invalid wire type.");
        }
    }

    /**
     * Verifies that the last call to readTag() returned the given tag value. This is used to verify that a nested group
     * ended with the correct end tag.
     *
     * @throws InvalidProtocolBufferException
     *             {@code value} does not match the last tag.
     */
    public void checkLastTagWas(final int value) throws InvalidProtocolBufferException {
        if (lastTag != value) {
            throw new InvalidProtocolBufferException("Protocol message end-group tag did not match expected tag.");
        }
    }

    /** Read a {@code double} field value from the stream. */
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readRawLittleEndian64());
    }

    /** Read a {@code float} field value from the stream. */
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readRawLittleEndian32());
    }

    /** Read a {@code uint64} field value from the stream. */
    public long readUInt64() throws IOException {
        return readRawVarint64();
    }

    /** Read an {@code int64} field value from the stream. */
    public long readInt64() throws IOException {
        return readRawVarint64();
    }

    /** Read an {@code int32} field value from the stream. */
    public int readInt32() throws IOException {
        return readRawVarint32();
    }

    /** Read a {@code fixed64} field value from the stream. */
    public long readFixed64() throws IOException {
        return readRawLittleEndian64();
    }

    /** Read a {@code fixed32} field value from the stream. */
    public int readFixed32() throws IOException {
        return readRawLittleEndian32();
    }

    /** Read a {@code bool} field value from the stream. */
    public boolean readBool() throws IOException {
        return readRawVarint32() != 0;
    }

    /** Read a raw Varint from the stream. */
    public long readRawVarint64() throws IOException {
        int shift = 0;
        long result = 0;
        while (shift < 64) {
            final byte b = buf.readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint.");
    }

    /**
     * Read a raw Varint from the stream. If larger than 32 bits, discard the upper bits.
     */
    public int readRawVarint32() throws IOException {
        byte tmp = buf.readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = buf.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = buf.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = buf.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = buf.readByte()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (buf.readByte() >= 0) {
                                return result;
                            }
                        }
                        throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint.");
                    }
                }
            }
        }
        return result;
    }

    /** Read a 32-bit little-endian integer from the stream. */
    public int readRawLittleEndian32() throws IOException {
        return buf.readIntLE();

    }

    /** Read a 64-bit little-endian integer from the stream. */
    public long readRawLittleEndian64() throws IOException {
        return buf.readLongLE();
    }

    public long readSFixed64() throws IOException {
        return readRawLittleEndian64();
    }

    /**
     * Reads and discards an entire message. This will read either until EOF or until an endgroup tag, whichever comes
     * first.
     */
    public void skipMessage() throws IOException {
        while (true) {
            final int tag = readTag();
            if (tag == 0 || !skipField(tag)) {
                return;
            }
        }
    }

    /**
     * Reads and discards {@code size} bytes.
     *
     * @throws InvalidProtocolBufferException
     *             The end of the stream or the current limit was reached.
     */
    public void skipRawBytes(final int size) throws IOException {
        if (size < 0) {
            throw new InvalidProtocolBufferException("CodedInputStream encountered an embedded string or message "
                    + "which claimed to have negative size.");
        }

        if (size > buf.readableBytes()) {
            throw new InvalidProtocolBufferException("While parsing a protocol message, the input ended unexpectedly "
                    + "in the middle of a field.  This could mean either than the "
                    + "input has been truncated or that an embedded message " + "misreported its own length.");
        }

        buf.readerIndex(buf.readerIndex() + size);
    }

    public int pushLimit(int byteLimit) throws InvalidProtocolBufferException {
        if (byteLimit < 0) {
            throw new InvalidProtocolBufferException("CodedInputStream encountered an embedded string or message"
                + " which claimed to have negative size.");
        }

        byteLimit += buf.readerIndex();
        final int oldLimit = buf.writerIndex();
        if (byteLimit > oldLimit) {
            throw new InvalidProtocolBufferException("While parsing a protocol message, the input ended unexpectedly"
                + " in the middle of a field.  This could mean either than the input has been truncated or that an"
                + " embedded message misreported its own length.");
        }
        buf.writerIndex(byteLimit);
        return oldLimit;
    }

    /**
     * Discards the current limit, returning to the previous limit.
     *
     * @param oldLimit The old limit, as returned by {@code pushLimit}.
     */
    public void popLimit(final int oldLimit) {
        buf.writerIndex(oldLimit);
    }

    public int getBytesUntilLimit() {
        return buf.readableBytes();
    }
}
