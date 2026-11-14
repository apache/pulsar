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
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import net.jpountz.lz4.LZ4Factory;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

public class CompressionCodecHeapToDirectTest {

    @DataProvider
    public Object[][] heapInputs() {
        return new Object[][] {
                {CompressionType.LZ4, 0},
                {CompressionType.LZ4, 1024},
                {CompressionType.LZ4, 4096},
                {CompressionType.SNAPPY, 0},
                {CompressionType.SNAPPY, 1024},
                {CompressionType.SNAPPY, 4096},
        };
    }

    @Test(dataProvider = "heapInputs")
    public void testEncodeHeapSliceIntoDirectBuffer(CompressionType type, int length)
            throws IOException, InvocationTargetException {
        byte[] payload = new byte[length];
        new Random(0x5eedL + length).nextBytes(payload);
        int padding = 8;
        ByteBuf source = Unpooled.buffer(length + 4 * padding)
                .writeZero(2 * padding).writeBytes(payload).writeZero(2 * padding)
                .slice(padding, length + 2 * padding).setIndex(padding, padding + length);
        AtomicReference<ByteBuf> allocated = new AtomicReference<>();
        try {
            int arrayOffset = source.arrayOffset();
            assertThat(arrayOffset).as("heap slice array offset").isPositive();
            byte[] sourceBytes = ByteBufUtil.getBytes(source, 0, source.capacity());

            ByteBufAllocator allocator = mock(ByteBufAllocator.class);
            doAnswer(invocation -> {
                ByteBuf target = Unpooled.directBuffer(invocation.getArgument(0), invocation.getArgument(1));
                allocated.set(target);
                ByteBuf wrappedTarget = mock(ByteBuf.class, delegatesTo(target));
                allocated.set(wrappedTarget);
                return wrappedTarget;
            }).when(allocator).buffer(anyInt(), anyInt());

            CompressionCodec codec = type == CompressionType.LZ4
                    ? new CompressionCodecLZ4(allocator) : new CompressionCodecSnappy(allocator);
            ByteBuf wrappedSource = mock(ByteBuf.class, delegatesTo(source));
            ByteBuf encoded = codec.encode(wrappedSource);
            ByteBuf wrappedTarget = allocated.get();
            if (hasHeapArrayBaseOffset() && wrappedTarget.hasMemoryAddress()) {
                verifyNoNioViews(wrappedSource);
                verifyNoNioViews(wrappedTarget);
            } else {
                int targetCapacity = wrappedTarget.capacity();
                verify(wrappedSource).nioBuffer(padding, length);
                verify(wrappedTarget).nioBuffer(0, targetCapacity);
            }

            byte[] compressed = ByteBufUtil.getBytes(encoded);
            byte[] decoded = type == CompressionType.LZ4
                    ? LZ4Factory.safeInstance().safeDecompressor().decompress(compressed, length)
                    : Snappy.uncompress(compressed);

            assertThat(encoded.isDirect()).as("encoded buffer is direct").isTrue();
            assertThat(decoded).as("payload decoded by the reference codec").isEqualTo(payload);
            assertThat(source.readerIndex()).as("source reader index").isEqualTo(padding);
            assertThat(source.writerIndex()).as("source writer index").isEqualTo(padding + length);
            assertThat(source.refCnt()).as("source references").isEqualTo(1);
            assertThat(ByteBufUtil.getBytes(source, 0, source.capacity())).as("source bytes").isEqualTo(sourceBytes);
        } finally {
            ByteBuf target = allocated.get();
            if (target != null) {
                target.release();
            }
            source.release();
        }
    }

    private static boolean hasHeapArrayBaseOffset() throws InvocationTargetException {
        try {
            var method = PlatformDependent.class.getMethod("byteArrayBaseOffset");
            return Modifier.isStatic(method.getModifiers()) && method.getReturnType() == long.class
                    && (long) method.invoke(null) >= 0;
        } catch (NoSuchMethodException | IllegalAccessException ignored) {
            return false;
        }
    }

    private static void verifyNoNioViews(ByteBuf buffer) {
        verify(buffer, never()).nioBuffer();
        verify(buffer, never()).nioBuffer(anyInt(), anyInt());
        verify(buffer, never()).internalNioBuffer(anyInt(), anyInt());
        verify(buffer, never()).nioBuffers();
        verify(buffer, never()).nioBuffers(anyInt(), anyInt());
    }
}
