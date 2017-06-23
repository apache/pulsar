/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.crc;

import static org.apache.pulsar.checksum.utils.NativeUtils.*;

import java.nio.ByteBuffer;

import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.impl.AbstractIncrementalIntHash;
import com.scurrilous.circe.params.CrcParameters;

/**
 * Implementation of CRC-32C using the SSE 4.2 CRC instruction.
 */
public final class Sse42Crc32C extends AbstractIncrementalIntHash implements IncrementalIntHash {

    private static final boolean SUPPORTED = checkSupported();

    private static boolean checkSupported() {
        try {
            loadLibraryFromJar("/lib/libpulsar-checksum." + libType());
            return nativeSupported();
        } catch (final Exception | UnsatisfiedLinkError e) {
            return false;
        }
    }

    /**
     * Returns whether SSE 4.2 CRC-32C is supported on this system.
     * 
     * @return true if this class is supported, false if not
     */
    public static boolean isSupported() {
        return SUPPORTED;
    }

    private final long config;

    Sse42Crc32C() {
        config = 0;
    }

    public Sse42Crc32C(int chunkWords[]) {
        if (chunkWords.length == 0) {
            config = 0;
        } else {
            config = allocConfig(chunkWords);
            if (config == 0)
                throw new RuntimeException("CRC32C configuration allocation failed");
        }
    }

    @Override
    protected void finalize() {
        if (config != 0)
            freeConfig(config);
    }

    @Override
    public String algorithm() {
        return CrcParameters.CRC32C.algorithm();
    }

    @Override
    public int length() {
        return 4;
    }

    @Override
    public boolean supportsUnsafe() {
        return true;
    }

    @Override
    public int calculate(long address, long length) {
        return nativeUnsafe(initial(), address, length, config);
    }

    @Override
    public int resume(int current, ByteBuffer input) {
        if (input.isDirect()) {
            final int result = nativeDirectBuffer(current, input, input.position(), input.remaining(), config);
            input.position(input.limit());
            return result;
        }

        return super.resume(current, input);
    }

    @Override
    public int resume(int current, long address, long length) {
        return nativeUnsafe(current, address, length, config);
    }

    @Override
    protected int initial() {
        return 0;
    }

    @Override
    protected int resumeUnchecked(int current, byte[] input, int index, int length) {
        return nativeArray(current, input, index, length, config);
    }

    private static native boolean nativeSupported();

    private static native int nativeArray(int current, byte[] input, int index, int length, long config);

    private static native int nativeDirectBuffer(int current, ByteBuffer input, int offset, int length, long config);

    private static native int nativeUnsafe(int current, long address, long length, long config);

    private static native long allocConfig(int[] chunkWords);

    private static native void freeConfig(long config);
}
