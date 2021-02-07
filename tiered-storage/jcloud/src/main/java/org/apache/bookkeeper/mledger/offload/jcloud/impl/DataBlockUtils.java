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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;

/**
 * Utility class for performing various Data Block functions including:
 *    - Calculating the data block offload key
 *    - Calculating the data block index key
 *    - Adding version metadata information to a Data Block
 *    - Validating the version metadata information of a Data Block.
 *  <p>
 *  Additional functions can be added in to future to tag Data Blocks with
 *  information such as the compression algorithm used to compress the contents,
 *  the md5 checksum of the content for validation, date published, etc.
 *  </p>
 */
public class DataBlockUtils {

    /**
     * Version checking marker interface.
     */
    public interface VersionCheck {
        void check(String key, Blob blob) throws IOException;
    }

    public static final String METADATA_FORMAT_VERSION_KEY = "S3ManagedLedgerOffloaderFormatVersion";
    static final String CURRENT_VERSION = String.valueOf(1);

    public static String dataBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d", uuid.toString(), ledgerId);
    }

    public static String indexBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d-index", uuid.toString(), ledgerId);
    }

    public static String indexBlockOffloadKey(UUID uuid) {
        return String.format("%s-index", uuid.toString());
    }

    public static void addVersionInfo(BlobBuilder blobBuilder, Map<String, String> userMetadata) {
        ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
        metadataBuilder.putAll(userMetadata);
        metadataBuilder.put(METADATA_FORMAT_VERSION_KEY.toLowerCase(), CURRENT_VERSION);
        blobBuilder.userMetadata(metadataBuilder.build());
    }

    public static final VersionCheck VERSION_CHECK = (key, blob) -> {
        // NOTE all metadata in jclouds comes out as lowercase, in an effort to normalize the providers
        String version = blob.getMetadata().getUserMetadata().get(METADATA_FORMAT_VERSION_KEY.toLowerCase());
        if (version == null || !version.equals(CURRENT_VERSION)) {
            throw new IOException(String.format("Invalid object version %s for %s, expect %s",
                version, key, CURRENT_VERSION));
        }
    };
}
