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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreManagedLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.JCloudBlobStoreFactory;
import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.JCloudBlobStoreFactoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class JCloudBlobStoreConfigurationFactoryTests {

    private static final Logger log = LoggerFactory.getLogger(JCloudBlobStoreConfigurationFactoryTests.class);
    public final static String BUCKET = "pulsar-unittest";
    
    private OrderedScheduler scheduler;
    private Properties props;
    
    @BeforeTest
    public final void init() {
        scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("offloader").build();
        props = new Properties();
    }
    
    @Test
    public final void testAWSS3AllProperties() throws Exception {
        props.put("managedLedgerOffloadDriver", "AWS_S3");
        props.put("region", "eu-west-1");
        props.put("bucket", BUCKET);
        props.put("maxBlockSizeInBytes", "9999999");
        props.put("readBufferSizeInBytes", "4");
        props.put("managedLedgerOffloadMaxThreads", "6");
        
        JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
        assertNotNull(conf);
        assertEquals(conf.getRegion(), "eu-west-1");
        assertEquals(conf.getBucket(), BUCKET);
        assertEquals(conf.getManagedLedgerOffloadMaxThreads(), 6);
        assertEquals(conf.getMaxBlockSizeInBytes(), 9999999);
        assertEquals(conf.getReadBufferSizeInBytes(), 4);
        assertEquals(conf.getProvider().getDriver(), "aws-s3");
    }
    
    @Test
    public void testAWSS3NoRegionConfigured() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "AWS_S3");
        props.put("s3ManagedLedgerOffloadBucket", BUCKET);
        
        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }

    @Test
    public void testAWSS3NoBucketConfigured() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "AWS_S3");
        props.put("region", "eu-west-1");
        
        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }

    @Test
    public void testAWSS3SmallBlockSizeConfigured() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "AWS_S3");
        props.put("region", "eu-west-1");
        props.put("bucket", BUCKET);
        props.put("maxBlockSizeInBytes", "1024");
       
        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }
    
    @Test
    public final void testAzureAllProperties() throws Exception {
        props.put("managedLedgerOffloadDriver", "AZURE_BLOB");
        props.put("region", "eu-west-1");
        props.put("bucket", BUCKET);
        props.put("azureStorageAccountName", "value");
        props.put("azureStorageAccountKey", "value");
        props.put("maxBlockSizeInBytes", "9999999");
        props.put("readBufferSizeInBytes", "4");
        props.put("managedLedgerOffloadMaxThreads", "6");
        
        JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
        assertNotNull(conf);
        assertEquals(conf.getRegion(), "eu-west-1");
        assertEquals(conf.getBucket(), BUCKET);
        assertEquals(conf.getManagedLedgerOffloadMaxThreads(), 6);
        assertEquals(conf.getMaxBlockSizeInBytes(), 9999999);
        assertEquals(conf.getReadBufferSizeInBytes(), 4);
        assertEquals(conf.getProvider().getDriver(), "azureblob");
    }
    
    @Test
    public void testAzureNoRegionConfigured() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "AZURE_BLOB");
        props.put("azureStorageAccountName", "value");
        props.put("azureStorageAccountKey", "value");
        props.put("bucket", BUCKET);
        
        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }

    @Test
    public void testAzureNoBucketConfigured() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "AZURE_BLOB");
        props.put("azureStorageAccountName", "value");
        props.put("azureStorageAccountKey", "value");
        props.put("region", "eu-west-1");
        
        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }
    
    @Test
    public void testAzureNoCredentialsConfigured() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "AZURE_BLOB");
        props.put("bucket", BUCKET);
        props.put("region", "eu-west-1");
        
        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }

    @Test
    public void testGcsNoKeyPath() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "GOOGLE");
        props.put("bucket", BUCKET);
       
        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
            log.error("Expected pse", pse);
        }
    }

    @Test
    public void testGcsNoBucketConfigured() throws Exception {
        
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "GOOGLE");
        File tmpKeyFile = File.createTempFile("gcsOffload", "json");
        props.put("gcsManagedLedgerOffloadServiceAccountKeyFile", tmpKeyFile.getAbsolutePath());

        try { 
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
            log.error("Expected pse", pse);
        }
    }

    @Test
    public void testGcsSmallBlockSizeConfigured() throws Exception {
        Properties props = new Properties();
        props.put("managedLedgerOffloadDriver", "GOOGLE");
        File tmpKeyFile = File.createTempFile("gcsOffload", "json");
        props.put("gcsManagedLedgerOffloadServiceAccountKeyFile", tmpKeyFile.getAbsolutePath());
        props.put("bucket", BUCKET);
        props.put("maxBlockSizeInBytes", 1024);

        try {
            JCloudBlobStoreFactory conf = JCloudBlobStoreFactoryFactory.create(props);
            BlobStoreManagedLedgerOffloader.create(conf, Maps.newHashMap(), scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
            log.error("Expected pse", pse);
        }
    }

}
