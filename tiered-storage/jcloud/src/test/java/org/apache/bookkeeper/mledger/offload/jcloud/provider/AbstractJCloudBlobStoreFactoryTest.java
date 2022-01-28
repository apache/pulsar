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
import java.util.List;
import java.util.Map;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;

public abstract class AbstractJCloudBlobStoreFactoryTest {
    
    protected static final int DEFAULT_BLOCK_SIZE = 5*1024*1024;
    protected static final int DEFAULT_READ_BUFFER_SIZE = 1*1024*1024;
    protected static final ByteSource PAYLOAD = ByteSource.wrap("blob-content".getBytes(Charsets.UTF_8));
    
    protected TieredStorageConfiguration config;
    protected BlobStore blobStore;
    
    protected abstract Map<String, String> getConfig();

    @BeforeTest
    protected final void init() throws Exception {
        config =  TieredStorageConfiguration.create(getConfig());
        config.getProvider().validate(config);
        blobStore = config.getBlobStore();
    }
    
    protected void sendBlob(String containerName, String blobName, ByteSource payload) throws IOException {
     // Create a Blob
        Blob blob = createBlob(payload, blobName);

        // Upload the Blob
        blobStore.putBlob(containerName, blob);
        Assert.assertTrue(blobStore.blobExists(containerName, blobName));
    }
    
    protected void verifyBlob(String containerName, String blobName, ByteSource payload) throws IOException {
        Blob retrieved = blobStore.getBlob(containerName, blobName);
        Assert.assertNotNull(retrieved);
        
        Payload p = retrieved.getPayload();
        Assert.assertEquals(IOUtils.toByteArray(p.openStream()), payload.read());
    }
    
    protected void sendAndVerifyBlob(String containerName, String blobName, ByteSource payload) throws IOException {
        sendBlob(containerName, blobName, payload);
        verifyBlob(containerName, blobName, payload);
    }
    
    protected void sendMultipartPayload(String containerName, String blobName, String[] lines) {
        MultipartUpload mpu = null;
        List<MultipartPart> parts = Lists.newArrayList();

        BlobBuilder blobBuilder = blobStore.blobBuilder(blobName);
        Blob blob = blobBuilder.build();
        mpu = blobStore.initiateMultipartUpload(containerName, blob.getMetadata(), new PutOptions());

        int partId = 1;
        for (String line: lines) {
            Payload partPayload = Payloads.newByteArrayPayload(line.getBytes());
            partPayload.getContentMetadata().setContentLength((long) line.length());
            partPayload.getContentMetadata().setContentType("application/octet-stream");
            parts.add(blobStore.uploadMultipartPart(mpu, partId++, partPayload));
        }

        blobStore.completeMultipartUpload(mpu, parts);
    }
    
    protected void deleteBlobAndVerify(String conatinerName, String blobName) {
        blobStore.removeBlob(conatinerName, blobName);
        Assert.assertFalse(blobStore.blobExists(conatinerName, blobName));
    }
    
    protected void deleteContainerAndVerify(String containerName) {
        blobStore.deleteContainer(containerName);
        Assert.assertFalse(blobStore.containerExists(containerName));
    }
    
    protected Blob createBlob(ByteSource payload, String name) throws IOException {
        return blobStore.blobBuilder(name) // The blob name?
                .payload(payload.read())
                .contentLength(payload.size())
                .build();
    }
}
