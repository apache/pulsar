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
package org.apache.bookkeeper.mledger;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang.StringUtils;

@Data
@AllArgsConstructor
@ToString
public class MetadataCompressionConfig {
    MLDataFormats.CompressionType compressionType;
    long compressSizeThresholdInBytes;

    public MetadataCompressionConfig(String compressionType) throws IllegalArgumentException {
        this(compressionType, 0);
    }

    public MetadataCompressionConfig(String compressionType, long compressThreshold) throws IllegalArgumentException {
        this.compressionType = parseCompressionType(compressionType);
        this.compressSizeThresholdInBytes = compressThreshold;
    }

    public static MetadataCompressionConfig noCompression =
            new MetadataCompressionConfig(MLDataFormats.CompressionType.NONE, 0);

    private MLDataFormats.CompressionType parseCompressionType(String value) throws IllegalArgumentException {
        if (StringUtils.isEmpty(value)) {
            return MLDataFormats.CompressionType.NONE;
        }

        MLDataFormats.CompressionType compressionType;
        compressionType = MLDataFormats.CompressionType.valueOf(value);

        return compressionType;
    }
}
