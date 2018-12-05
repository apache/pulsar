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
package org.apache.bookkeeper.mledger.offload.jcloud.provider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jclouds.blobstore.BlobStore;

/**
 * Centralized storage location for all the Blobstore instances
 * created and used for tiered-storage.
 * <p>
 * Users should first check to see if an existing Blobstore for the
 * the given BlobStoreKey exists, and if so, use the existing one.
 * Otherwise a new one will be created and added to the Repository
 * </p>
 */
public class BlobStoreRepository {

    private static final ConcurrentMap<BlobStoreLocation, BlobStore> blobStores = new ConcurrentHashMap<>();

    public static boolean containsKey(BlobStoreLocation key) {
        return blobStores.containsKey(key);
    }

    /**
     * If a BlobStore for the given BlobStoreLocation already exists then return it,
     * otherwise return null.
     *
     * @param config
     * @return
     */
    public static BlobStore get(BlobStoreLocation key) {
        return blobStores.get(key);
    }

    /**
     * If a BlobStore for the given configuration already exists then return it,
     * otherwise create a new one.
     *
     * @param config
     * @return
     */
    public static BlobStore getOrCreate(TieredStorageConfiguration config) {
        if (!blobStores.containsKey(config.getBlobStoreLocation())) {
            blobStores.putIfAbsent(config.getBlobStoreLocation(), config.getBlobStore());
        }
        return blobStores.get(config.getBlobStoreLocation());
    }

    /**
     * Allow you to overwrite an existing value in the BlobStore Repository.
     *
     * @param key
     * @param blobStore
     */
    public static void put(BlobStoreLocation key, BlobStore blobStore) {
        blobStores.put(key, blobStore);
    }

    /**
     * Clears the contents of the BlobStoreRepository.
     */
    public static void clear() {
        blobStores.entrySet().stream().forEach(e -> e.getValue().getContext().close());
        blobStores.clear();
    }
}