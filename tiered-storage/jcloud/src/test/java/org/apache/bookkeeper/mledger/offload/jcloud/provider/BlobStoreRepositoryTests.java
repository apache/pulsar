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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.jclouds.blobstore.BlobStore;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class BlobStoreRepositoryTests {
    
    private Map<String, String> metaData;
    private TieredStorageConfiguration config;
    
    @BeforeTest
    public final void init() {
        BlobStoreRepository.clear();
        
        metaData = new HashMap<String, String> ();
        metaData.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "TRANSIENT");
        metaData.put(TieredStorageConfiguration.METADATA_FIELD_REGION, "");
        metaData.put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, "my-bucket");
        metaData.put(TieredStorageConfiguration.METADATA_FIELD_ENDPOINT, "");
        
        config = TieredStorageConfiguration.create(metaData);
    }

    @Test
    public final void getOrCreateTest() {
        BlobStore bs = BlobStoreRepository.getOrCreate(config);
        Assert.assertNotNull(bs);
    }
    
    @Test
    public final void mocksTest() {
        
        BlobStore spiedBlobStore = mock(BlobStore.class);
        Mockito
            .doThrow(new RuntimeException("Blam"))
            .when(spiedBlobStore).initiateMultipartUpload(any(), any(), any());
        
        JCloudBlobStoreProvider mockedProvider = mock(JCloudBlobStoreProvider.class);
        when(mockedProvider.getProviderMetadata()).thenReturn(JCloudBlobStoreProvider.TRANSIENT.getProviderMetadata());
        when(mockedProvider.getBlobStore(any(TieredStorageConfiguration.class))).thenReturn(spiedBlobStore);
        config.setProvider(mockedProvider);
        
        BlobStore bs = BlobStoreRepository.getOrCreate(config);
        Assert.assertNotNull(bs);
        Assert.assertEquals(bs, spiedBlobStore);
        
        Assert.assertEquals(BlobStoreRepository.get(config.getBlobStoreLocation()), spiedBlobStore);
    }
}
