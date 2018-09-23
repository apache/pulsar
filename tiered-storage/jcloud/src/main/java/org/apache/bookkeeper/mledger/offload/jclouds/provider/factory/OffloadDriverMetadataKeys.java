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
package org.apache.bookkeeper.mledger.offload.jclouds.provider.factory;

import java.io.IOException;

import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreManagedLedgerOffloader.VersionCheck;

/**
 * Collection of constants used to track Offload metadata keys.
 */
public interface OffloadDriverMetadataKeys {

    String METADATA_FIELD_BLOB_STORE_PROVIDER = "provider";
    String METADATA_FIELD_BUCKET = "bucket";
    String METADATA_FIELD_REGION = "region";
    String METADATA_FIELD_ENDPOINT = "endpoint";

    // Use these keys for all Storage Providers
    String METADATA_FORMAT_VERSION_KEY = "ManagedLedgerOffloaderFormatVersion";
    String CURRENT_VERSION = String.valueOf(1);

    VersionCheck VERSION_CHECK = (key, blob) -> {
        // NOTE all metadata in jclouds comes out as lowercase, in an effort to normalize the providers
        String version = blob.getMetadata().getUserMetadata().get(METADATA_FORMAT_VERSION_KEY.toLowerCase());
        if (version == null || !version.equals(CURRENT_VERSION)) {
            throw new IOException(String.format("Invalid object version %s for %s, expect %s",
                version, key, CURRENT_VERSION));
        }
    };
}