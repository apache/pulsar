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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.domain.Blob;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TransientBlobStoreFactoryTests extends AbstractJCloudBlobStoreFactoryTest {

    private static final String TEST_CONTAINER_NAME = "test-container";
    private static final String TEST_BLOB_NAME = "test-blob";
    
    @Test
    public final void smallBlobTest() throws IOException {
        sendAndVerifyBlob(TEST_CONTAINER_NAME, TEST_BLOB_NAME, PAYLOAD);
        deleteBlobAndVerify(TEST_CONTAINER_NAME, "test-blob");
        deleteContainerAndVerify(TEST_CONTAINER_NAME);
    }
    
    @Test
    public final void multipartUploadTest() throws IOException {
        String[] lines = new String[] { "Line 1", "Line 2", "Line 3"};
        sendMultipartPayload(TEST_CONTAINER_NAME, TEST_BLOB_NAME, lines );
        Assert.assertTrue(blobStore.blobExists(TEST_CONTAINER_NAME, TEST_BLOB_NAME));
        
        Blob retrieved = blobStore.getBlob(TEST_CONTAINER_NAME, TEST_BLOB_NAME);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(new String(IOUtils.toByteArray(retrieved.getPayload().openStream())), "Line 1Line 2Line 3");
    }

    @Override
    protected Map<String, String> getConfig() {
        Map<String, String> metadata = new HashMap<String,String>();
        metadata.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "transient");
        metadata.put("managedLedgerOffloadBucket", TEST_CONTAINER_NAME);
        return metadata;
    }
}
