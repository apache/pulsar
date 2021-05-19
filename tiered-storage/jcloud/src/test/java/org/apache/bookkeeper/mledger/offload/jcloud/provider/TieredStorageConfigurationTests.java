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

import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jclouds.domain.Credentials;
import org.testng.annotations.Test;

public class TieredStorageConfigurationTests {

    /*
     * Previous property names, for backwards-compatibility.
     */
    static final String BC_S3_REGION = "s3ManagedLedgerOffloadRegion";
    static final String BC_S3_BUCKET = "s3ManagedLedgerOffloadBucket";
    static final String BC_S3_ENDPOINT = "s3ManagedLedgerOffloadServiceEndpoint";
    static final String BC_S3_MAX_BLOCK_SIZE = "s3ManagedLedgerOffloadMaxBlockSizeInBytes";
    static final String BC_S3_READ_BUFFER_SIZE = "s3ManagedLedgerOffloadReadBufferSizeInBytes";

    static final String BC_GCS_BUCKET = "gcsManagedLedgerOffloadBucket";
    static final String BC_GCS_REGION = "gcsManagedLedgerOffloadRegion";
    static final String BC_GCS_MAX_BLOCK_SIZE = "gcsManagedLedgerOffloadMaxBlockSizeInBytes";
    static final String BC_GCS_READ_BUFFER_SIZE = "gcsManagedLedgerOffloadReadBufferSizeInBytes";

   
    /**
     * Confirm that both property options are available for AWS
     */
    @Test
    public final void awsS3KeysTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        List<String> keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_BUCKET);
        assertEquals(keys.get(0), BC_S3_BUCKET);
        assertEquals(keys.get(1), "managedLedgerOffloadBucket");
        
        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_REGION);
        assertEquals(keys.get(0), BC_S3_REGION);
        assertEquals(keys.get(1), "managedLedgerOffloadRegion");
        
        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_ENDPOINT);
        assertEquals(keys.get(0), BC_S3_ENDPOINT);
        assertEquals(keys.get(1), "managedLedgerOffloadServiceEndpoint");
        
        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE);
        assertEquals(keys.get(0), BC_S3_MAX_BLOCK_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadMaxBlockSizeInBytes");
        
        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_READ_BUFFER_SIZE);
        assertEquals(keys.get(0), BC_S3_READ_BUFFER_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadReadBufferSizeInBytes");
    }
    
    /**
     * Confirm that we can configure AWS using the new properties
     */
    @Test
    public final void awsS3PropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadBucket", "test bucket");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "1");
        map.put("managedLedgerOffloadReadBufferSizeInBytes", "500");
        map.put("managedLedgerOffloadServiceEndpoint", "http://some-url:9093");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        
        assertEquals(config.getRegion(), "us-east-1");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), new Integer(1));
        assertEquals(config.getReadBufferSizeInBytes(), new Integer(500));
        assertEquals(config.getServiceEndpoint(), "http://some-url:9093");
    }
    
    /**
     * Confirm that we can configure AWS using the old properties
     */
    @Test
    public final void awsS3BackwardCompatiblePropertiesTest() {
        Map<String, String> map = new HashMap<String,String>(); 
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put(BC_S3_BUCKET, "test bucket");
        map.put(BC_S3_ENDPOINT, "http://some-url:9093");
        map.put(BC_S3_MAX_BLOCK_SIZE, "12");
        map.put(BC_S3_READ_BUFFER_SIZE, "500");
        map.put(BC_S3_REGION, "test region");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        
        assertEquals(config.getRegion(), "test region");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), new Integer(12));
        assertEquals(config.getReadBufferSizeInBytes(), new Integer(500));
        assertEquals(config.getServiceEndpoint(), "http://some-url:9093");
    }

    /**
     * Confirm that with AWS we create different instances of the credentials
     * object each time we call the supplier, this ensure that we get fresh credentials
     * if the aws credential provider changes
     */
    @Test
    public final void awsS3CredsProviderTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);

        // set the aws properties with fake creds so the defaultProviderChain works
        System.setProperty("aws.accessKeyId", "fakeid1");
        System.setProperty("aws.secretKey", "fakekey1");
        Credentials creds1 = config.getProviderCredentials().get();
        assertEquals(creds1.identity, "fakeid1");
        assertEquals(creds1.credential, "fakekey1");

        // reset the properties and ensure we get different values by re-evaluating the chain
        System.setProperty("aws.accessKeyId", "fakeid2");
        System.setProperty("aws.secretKey", "fakekey2");
        Credentials creds2 = config.getProviderCredentials().get();
        assertEquals(creds2.identity, "fakeid2");
        assertEquals(creds2.credential, "fakekey2");

        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretKey");
    }

    /**
     * Confirm that both property options are available for GCS
     */
    @Test
    public final void gcsKeysTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE.getDriver());
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        List<String> keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_BUCKET);
        assertEquals(keys.get(0), BC_GCS_BUCKET);
        assertEquals(keys.get(1), "managedLedgerOffloadBucket");
        
        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_REGION);
        assertEquals(keys.get(0), BC_GCS_REGION);
        assertEquals(keys.get(1), "managedLedgerOffloadRegion");
        
        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE);
        assertEquals(keys.get(0), BC_GCS_MAX_BLOCK_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadMaxBlockSizeInBytes");
        
        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_READ_BUFFER_SIZE);
        assertEquals(keys.get(0), BC_GCS_READ_BUFFER_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadReadBufferSizeInBytes");
    }
    
    /**
     * Confirm that we can configure GCS using the new properties
     */
    @Test
    public final void gcsPropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadBucket", "test bucket");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "1");
        map.put("managedLedgerOffloadReadBufferSizeInBytes", "500");
        map.put("managedLedgerOffloadServiceEndpoint", "http://some-url:9093");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        
        assertEquals(config.getRegion(), "us-east-1");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), new Integer(1));
        assertEquals(config.getReadBufferSizeInBytes(), new Integer(500));
    }
    
    /**
     * Confirm that we can configure GCS using the old properties
     */
    @Test
    public final void gcsBackwardCompatiblePropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE.getDriver());
        map.put(BC_GCS_BUCKET, "test bucket");
        map.put(BC_GCS_MAX_BLOCK_SIZE, "12");
        map.put(BC_GCS_READ_BUFFER_SIZE, "500");
        map.put(BC_GCS_REGION, "test region");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        
        assertEquals(config.getRegion(), "test region");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), new Integer(12));
        assertEquals(config.getReadBufferSizeInBytes(), new Integer(500));
    }
}
