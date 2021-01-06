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
package org.apache.pulsar.common.compression;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Exposes ZstdFrameDecompressor which is package protected.
 */
public class ZStdRawDecompressor {


    private static final Constructor<?> constructor;
    private static final Method decompressMethod;

    private final Object instance;

    static {
        try {
            Class<?> frameDecompressor = ZStdRawCompressor.class.getClassLoader()
                    .loadClass("io.airlift.compress.zstd.ZstdFrameDecompressor");
            constructor = frameDecompressor.getConstructor();
            constructor.setAccessible(true);
            decompressMethod = frameDecompressor.getDeclaredMethod("decompress",
                    Object.class, long.class, long.class, Object.class, long.class, long.class);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public ZStdRawDecompressor() {
        try {
            instance = constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    int decompress(final Object inputBase, final long inputAddress, final long inputLimit,
                   final Object outputBase, final long outputAddress, final long outputLimit) {
        try {
            return (int) decompressMethod.invoke(instance, inputBase, inputAddress, inputLimit,
                    outputBase, outputAddress, outputLimit);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
