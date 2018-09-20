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

import java.util.EnumMap;

import lombok.experimental.UtilityClass;

import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;

@UtilityClass
public class CompressionCodecProvider {
    private static final EnumMap<CompressionType, CompressionCodec> codecs;

    static {
        codecs = new EnumMap<>(CompressionType.class);
        codecs.put(CompressionType.NONE, new CompressionCodecNone());
        codecs.put(CompressionType.LZ4, new CompressionCodecLZ4());
        codecs.put(CompressionType.ZLIB, new CompressionCodecZLib());
    }

    public static CompressionCodec getCompressionCodec(CompressionType type) {
        return codecs.get(type);
    }
}
