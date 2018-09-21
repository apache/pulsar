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
package org.apache.bookkeeper.mledger.offload.jcloud;

import java.util.Properties;

import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.JCloudBlobStoreFactoryFactory;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlobStoreTestBase {

    public final static String BUCKET = "pulsar-unittest";

    protected BlobStoreContext context = null;
    protected BlobStore blobStore = null;
    protected Properties blobStoreProviderProps = new Properties();

    @BeforeMethod
    public void start() throws Exception {
        if (Boolean.parseBoolean(System.getProperty("testRealAWS", "false"))) {
            log.info("TestReal AWS S3, bucket: {}", BUCKET);
            // To use this, must config credentials using "aws_access_key_id" as S3ID,
            // and "aws_secret_access_key" as S3Key. And bucket should exist in default region. e.g.
            //        props.setProperty("S3ID", "AXXXXXXQ");
            //        props.setProperty("S3Key", "HXXXXXß");
            blobStoreProviderProps.put(JCloudBlobStoreFactoryFactory.BLOB_STORE_PROVIDER_KEY, "AWS_S3");
            
            context = ContextBuilder.newBuilder("aws-s3")
                .credentials(System.getProperty("S3ID"), System.getProperty("S3Key"))
                .build(BlobStoreContext.class);
            blobStore = context.getBlobStore();
            // To use this, ~/.aws must be configured with credentials and a default region
            //s3client = AmazonS3ClientBuilder.standard().build();
        } else if (Boolean.parseBoolean(System.getProperty("testRealGCS", "false"))) {
            log.info("TestReal GCS, bucket: {}", BUCKET);
            // To use this, must config credentials using "client_email" as GCSID and "private_key" as GCSKey.
            // And bucket should exist in default region. e.g.
            //        props.setProperty("GCSID", "5XXXXXXXXXX6-compute@developer.gserviceaccount.com");
            //        props.setProperty("GCSKey", "XXXXXX");
            context = ContextBuilder.newBuilder("google-cloud-storage")
                .credentials(System.getProperty("GCSID"), System.getProperty("GCSKey"))
                .build(BlobStoreContext.class);
            blobStore = context.getBlobStore();
        } else if (Boolean.parseBoolean(System.getProperty("testRealAzure", "false"))) {
           log.info("TestReal Azure, bucket: {}", BUCKET);
            // To use this, must config credentials using "storageAccountName" as AzureID,
            // and "storageAccountKey" as AzureKey. And bucket should exist in default region. e.g.
            //        props.setProperty("AzureID", "<Your storage account name>");  
            //        props.setProperty("AzureKey", "<Your storage account primary access key>");
            context = ContextBuilder.newBuilder("azureblob")
                    .credentials(System.getProperty("AzureStorageID"), System.getProperty("AzureStorageKey"))
                    .buildView(BlobStoreContext.class);
            blobStore = context.getBlobStore();
        } else {
            log.info("Test Transient, bucket: {}", BUCKET);
            context = ContextBuilder.newBuilder("transient").build(BlobStoreContext.class);
            blobStore = context.getBlobStore();
            boolean create = blobStore.createContainerInLocation(null, BUCKET);
            log.debug("TestBase Create Bucket: {}, in blobStore, result: {}", BUCKET, create);
        }
    }

    @AfterMethod
    public void tearDown() {
        if (blobStore != null &&
            (Boolean.parseBoolean(System.getProperty("testRealAWS", "false")) ||
             Boolean.parseBoolean(System.getProperty("testRealGCS", "false")) || 
             Boolean.parseBoolean(System.getProperty("testRealAzure", "false")))) {
            blobStore.deleteContainer(BUCKET);
        }

        if (context != null) {
            context.close();
        }
    }

}
