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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Expose ZstdFrameCompressor which is a package protected class.
 */
public class ZStdRawCompressor {

    private final static Method compressMethod;

    static {
        try {
            Class<?> frameCompressor = ZStdRawCompressor.class.getClassLoader()
                    .loadClass("io.airlift.compress.zstd.ZstdFrameCompressor");
            compressMethod = frameCompressor.getDeclaredMethod("compress", Object.class, long.class, long.class,
                    Object.class, long.class, long.class, int.class);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static int compress(long inputAddress, long inputLimit,
                               long outputAddress, long outputLimit, int compressionLevel) {
        try {
            return (int) compressMethod.invoke(null, null, inputAddress,
                    inputLimit, null, outputAddress, outputLimit, compressionLevel);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }


}
