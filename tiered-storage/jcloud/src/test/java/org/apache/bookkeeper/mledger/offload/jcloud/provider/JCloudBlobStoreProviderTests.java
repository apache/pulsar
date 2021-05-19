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
import java.util.Map;

import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.testng.annotations.Test;

public class JCloudBlobStoreProviderTests {
      
    private TieredStorageConfiguration config;
    
    @Test
    public void awsValidationSuccessTest() {
        Map<String, String> map = new HashMap<String,String>(); 
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadBucket", "test bucket");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "99999999");
        config = new TieredStorageConfiguration(map);
        JCloudBlobStoreProvider.AWS_S3.validate(config);
    }
    
    @Test
    public void awsValidationDefaultBlockSizeTest() {
        Map<String, String> map = new HashMap<String,String>(); 
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadBucket", "test bucket");
        config = new TieredStorageConfiguration(map);
        JCloudBlobStoreProvider.AWS_S3.validate(config);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Either Region or ServiceEndpoint must specified for aws-s3 offload")
    public void awsValidationMissingRegionTest() {
        Map<String, String> map = new HashMap<String,String>(); 
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put("managedLedgerOffloadBucket", "my-bucket");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "999999");
        config = new TieredStorageConfiguration(map);
        JCloudBlobStoreProvider.AWS_S3.validate(config);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Bucket cannot be empty for aws-s3 offload")
    public void awsValidationMissingBucketTest() {
        Map<String, String> map = new HashMap<String,String>(); 
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "99999999");
        config = new TieredStorageConfiguration(map);
        assertEquals(config.getRegion(), "us-east-1");
        JCloudBlobStoreProvider.AWS_S3.validate(config);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "ManagedLedgerOffloadMaxBlockSizeInBytes cannot "
                    + "be less than 5MB for aws-s3 offload")
    public void awsValidationBlockSizeTest() {
        Map<String, String> map = new HashMap<String,String>(); 
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadBucket", "test bucket");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "1");
        config = new TieredStorageConfiguration(map);
        JCloudBlobStoreProvider.AWS_S3.validate(config);
    }
   
    @Test
    public void transientValidationSuccessTest() {
        Map<String, String> map = new HashMap<String,String>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "transient");
        map.put("managedLedgerOffloadBucket", "test bucket");
        config = new TieredStorageConfiguration(map);
        JCloudBlobStoreProvider.TRANSIENT.validate(config);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Bucket cannot be empty for Local offload")
    public void transientValidationFailureTest() {
        Map<String, String> map = new HashMap<String,String>(); 
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "transient");
        config = new TieredStorageConfiguration(map);
        JCloudBlobStoreProvider.TRANSIENT.validate(config);
    }
}
