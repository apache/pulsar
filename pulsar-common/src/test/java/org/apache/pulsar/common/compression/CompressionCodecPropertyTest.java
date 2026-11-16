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
package org.apache.pulsar.common.compression;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.DataFormatException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import net.jpountz.lz4.LZ4Factory;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

/**
 * Generated properties for the buffer paths used by producers, consumers, compaction and metadata storage.
 * Failures include the seed and trial; set PULSAR_COMPRESSION_TEST_SEED to replay one seed.
 */
public class CompressionCodecPropertyTest {

    private static final int RANDOM_TRIALS = 16;
    private static final long DEFAULT_SEED = 0x5eed5eedL;
    private static final int[] BOUNDARY_LENGTHS = {
            0, 1, 4095, 4096, 4097, 65535, 65536, 65537, 131071, 131072, 131073
    };

    private enum Scenario {
        // MessageImpl wraps the schema's ByteBuffer, including its position and limit.
        HEAP_MESSAGE,
        DIRECT_MESSAGE,
        // BatchMessageContainerImpl and RawBatchConverter compress an allocator-owned buffer.
        BATCH,
        // MetaStoreImpl decompresses heap bytes after skipping the metadata header.
        METADATA,
        // RawMessageImpl and managed-ledger entries expose retained views.
        COMPACTED_ENTRY_SLICE,
        COMPACTED_ENTRY_DUPLICATE,
        // ConsumerImpl decrypts and reassembles chunks into contiguous allocator-owned buffers.
        DECRYPTED_OR_CHUNKED_MESSAGE,
        // Commands.addBrokerEntryMetadata prepends a component; the payload remains in the original component.
        BROKER_ENTRY_METADATA
    }

    private enum Pattern {
        RANDOM,
        CONSTANT,
        REPEATING,
        SPARSE
    }

    @DataProvider
    public Object[][] codecsAndScenarios() {
        String configuredSeed = System.getenv("PULSAR_COMPRESSION_TEST_SEED");
        long seed = configuredSeed == null ? DEFAULT_SEED : Long.decode(configuredSeed);
        List<Object[]> cases = new ArrayList<>();
        for (CompressionType type : CompressionType.values()) {
            for (Scenario scenario : Scenario.values()) {
                cases.add(new Object[] {type, scenario, seed});
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "codecsAndScenarios")
    public void testBufferAndCompatibilityProperties(CompressionType type, Scenario scenario, long seed)
            throws Exception {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);
        Random random = new Random(seed);
        Pattern[] patterns = Pattern.values();
        int boundaryTrials = BOUNDARY_LENGTHS.length * patterns.length;
        for (int trial = 0; trial < boundaryTrials + RANDOM_TRIALS; trial++) {
            Pattern pattern = trial < boundaryTrials
                    ? patterns[trial % patterns.length] : patterns[random.nextInt(patterns.length)];
            int length = trial < boundaryTrials
                    ? BOUNDARY_LENGTHS[trial / patterns.length] : random.nextInt(131073);
            byte[] payload = payload(random, length, pattern);
            String context = "codec=" + type + ", scenario=" + scenario + ", seed=" + seed
                    + ", trial=" + trial + ", length=" + length + ", pattern=" + pattern;
            try {
                assertProperties(codec, type, scenario, payload, random);
            } catch (Exception | AssertionError failure) {
                throw new AssertionError(context, failure);
            }
        }
    }

    private static void assertProperties(CompressionCodec codec, CompressionType type, Scenario scenario,
                                         byte[] payload, Random random) throws IOException, DataFormatException {
        byte[] referenceEncoded = referenceEncode(type, payload);
        ByteBuf input = encodeInput(scenario, payload, random);
        ByteBuf encoded = null;
        try {
            BufferState before = BufferState.capture(input);
            encoded = codec.encode(input);
            byte[] actualEncoded = ByteBufUtil.getBytes(encoded);
            assertThat(referenceDecode(type, actualEncoded, payload.length))
                    .as("production encoding decoded by the independent implementation").isEqualTo(payload);
            encoded.release();
            encoded = null;
            before.assertRestored(input);

            assertDecoded(codec, scenario, actualEncoded, payload, random, "production");
            assertDecoded(codec, scenario, referenceEncoded, payload, random, "reference");
        } finally {
            if (encoded != null) {
                encoded.release();
            }
            input.release();
        }
    }

    private static void assertDecoded(CompressionCodec codec, Scenario scenario, byte[] encodedBytes,
                                      byte[] payload, Random random, String encoder) throws IOException {
        ByteBuf input = decodeInput(scenario, encodedBytes, random);
        ByteBuf decoded = null;
        try {
            BufferState before = BufferState.capture(input);
            decoded = codec.decode(input, payload.length);
            assertThat(decoded.readableBytes()).as("decoded length").isEqualTo(payload.length);
            assertThat(ByteBufUtil.getBytes(decoded)).as("decoded contents").isEqualTo(payload);
            // NONE returns a retained alias: release each returned reference before checking the input's count.
            decoded.release();
            decoded = null;
            before.assertRestored(input);
        } catch (Exception | AssertionError failure) {
            throw new AssertionError("decoding " + encoder + " encoding", failure);
        } finally {
            if (decoded != null) {
                decoded.release();
            }
            input.release();
        }
    }

    private static byte[] payload(Random random, int length, Pattern pattern) {
        byte[] bytes = new byte[length];
        switch (pattern) {
            case RANDOM -> random.nextBytes(bytes);
            case CONSTANT -> Arrays.fill(bytes, (byte) random.nextInt(256));
            case REPEATING -> {
                byte[] motif = new byte[1 + random.nextInt(64)];
                random.nextBytes(motif);
                for (int i = 0; i < bytes.length; i++) {
                    bytes[i] = motif[i % motif.length];
                }
            }
            case SPARSE -> {
                for (int i = 0; i < length / 128; i++) {
                    bytes[random.nextInt(length)] = (byte) random.nextInt(256);
                }
            }
            default -> throw new AssertionError(pattern);
        }
        return bytes;
    }

    private static ByteBuf encodeInput(Scenario scenario, byte[] data, Random random) {
        return switch (scenario) {
            case HEAP_MESSAGE -> messageBuffer(data, random, false);
            case DIRECT_MESSAGE -> messageBuffer(data, random, true);
            case BATCH, COMPACTED_ENTRY_SLICE, COMPACTED_ENTRY_DUPLICATE -> batchBuffer(data, random);
            case METADATA, DECRYPTED_OR_CHUNKED_MESSAGE, BROKER_ENTRY_METADATA ->
                    Unpooled.wrappedBuffer(data.clone());
        };
    }

    private static ByteBuf decodeInput(Scenario scenario, byte[] data, Random random) {
        return switch (scenario) {
            case HEAP_MESSAGE, DIRECT_MESSAGE, BATCH -> paddedBuffer(data, random, true);
            case METADATA -> paddedBuffer(data, random, false);
            case COMPACTED_ENTRY_SLICE -> entryView(data, random, false);
            case COMPACTED_ENTRY_DUPLICATE -> entryView(data, random, true);
            case DECRYPTED_OR_CHUNKED_MESSAGE ->
                    PulsarByteBufAllocator.DEFAULT.buffer(data.length).writeBytes(data);
            case BROKER_ENTRY_METADATA -> compositePayload(data, random);
        };
    }

    private static ByteBuf messageBuffer(byte[] data, Random random, boolean direct) {
        int prefix = 2 + random.nextInt(31);
        int suffix = 2 + random.nextInt(31);
        byte[] storage = new byte[prefix + data.length + suffix];
        random.nextBytes(storage);
        ByteBuffer message = direct ? ByteBuffer.allocateDirect(storage.length) : ByteBuffer.allocate(storage.length);
        message.put(storage);
        message.position(prefix);
        message.put(data);
        message.limit(prefix + data.length).position(prefix);
        return Unpooled.wrappedBuffer(message);
    }

    private static ByteBuf batchBuffer(byte[] data, Random random) {
        int suffix = 2 + random.nextInt(31);
        ByteBuf buffer = PulsarByteBufAllocator.DEFAULT.buffer(data.length + suffix);
        buffer.writeBytes(data);
        writePadding(buffer, suffix, random);
        // Batches normally have unused capacity; only bytes before writerIndex belong to the message.
        return buffer.writerIndex(data.length);
    }

    private static ByteBuf paddedBuffer(byte[] data, Random random, boolean direct) {
        int prefix = 2 + random.nextInt(31);
        int suffix = 2 + random.nextInt(31);
        ByteBuf buffer = direct ? PulsarByteBufAllocator.DEFAULT.directBuffer(prefix + data.length + suffix)
                : Unpooled.buffer(prefix + data.length + suffix);
        writePadding(buffer, prefix, random);
        buffer.writeBytes(data);
        writePadding(buffer, suffix, random);
        return buffer.setIndex(prefix, prefix + data.length);
    }

    private static ByteBuf entryView(byte[] data, Random random, boolean duplicate) {
        ByteBuf entry = paddedBuffer(data, random, true);
        try {
            if (duplicate) {
                return entry.retainedDuplicate();
            }
            int offset = entry.readerIndex() / 2;
            return entry.retainedSlice(offset, entry.capacity() - offset)
                    .setIndex(entry.readerIndex() - offset, entry.writerIndex() - offset);
        } finally {
            entry.release();
        }
    }

    private static void writePadding(ByteBuf buffer, int length, Random random) {
        for (int i = 0; i < length; i++) {
            buffer.writeByte(random.nextInt(256));
        }
    }

    private static ByteBuf compositePayload(byte[] data, Random random) {
        CompositeByteBuf composite = Unpooled.compositeBuffer(2);
        try {
            int headerSize = 2 + random.nextInt(31);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer(headerSize);
            writePadding(header, headerSize, random);
            composite.addComponent(true, header);
            ByteBuf payload = paddedBuffer(data, random, true);
            int readerIndex = headerSize + payload.readerIndex();
            int writerIndex = headerSize + payload.writerIndex();
            payload.setIndex(0, payload.capacity());
            composite.addComponent(true, payload);
            return composite.setIndex(readerIndex, writerIndex);
        } catch (RuntimeException | Error failure) {
            composite.release();
            throw failure;
        }
    }

    // Array APIs in independent implementations prevent matching production encoder/decoder bugs from hiding.
    private static byte[] referenceEncode(CompressionType type, byte[] payload) throws IOException {
        return switch (type) {
            case NONE -> payload.clone();
            case LZ4 -> LZ4Factory.safeInstance().fastCompressor().compress(payload);
            case ZSTD -> Zstd.compress(payload, 3);
            case SNAPPY -> Snappy.compress(payload);
            case ZLIB -> {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                try (DeflaterOutputStream stream = new DeflaterOutputStream(output)) {
                    stream.write(payload);
                }
                yield output.toByteArray();
            }
        };
    }

    private static byte[] referenceDecode(CompressionType type, byte[] encoded, int length)
            throws IOException, DataFormatException {
        return switch (type) {
            case NONE -> encoded.clone();
            case LZ4 -> LZ4Factory.safeInstance().safeDecompressor().decompress(encoded, length);
            case ZSTD -> Zstd.decompress(encoded, length);
            case SNAPPY -> Snappy.uncompress(encoded);
            case ZLIB -> {
                Inflater inflater = new Inflater();
                try {
                    inflater.setInput(encoded);
                    byte[] output = new byte[length];
                    assertThat(inflater.inflate(output)).as("reference decoded length").isEqualTo(length);
                    yield output;
                } finally {
                    inflater.end();
                }
            }
        };
    }

    private record BufferState(int readerIndex, int writerIndex, int refCount, byte[] contents) {
        static BufferState capture(ByteBuf buffer) {
            byte[] contents = new byte[buffer.capacity()];
            buffer.getBytes(0, contents);
            return new BufferState(buffer.readerIndex(), buffer.writerIndex(), buffer.refCnt(), contents);
        }

        void assertRestored(ByteBuf buffer) {
            assertThat(buffer.readerIndex()).as("input reader index").isEqualTo(readerIndex);
            assertThat(buffer.writerIndex()).as("input writer index").isEqualTo(writerIndex);
            assertThat(buffer.refCnt()).as("input reference count after releasing output").isEqualTo(refCount);
            byte[] actual = new byte[buffer.capacity()];
            buffer.getBytes(0, actual);
            assertThat(actual).as("input contents including padding").isEqualTo(contents);
        }
    }
}
