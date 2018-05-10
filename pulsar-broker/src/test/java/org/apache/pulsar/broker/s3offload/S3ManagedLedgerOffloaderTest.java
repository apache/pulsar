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
package org.apache.pulsar.broker.s3offload;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import io.findify.s3mock.S3Mock;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.MockBookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

class S3ManagedLedgerOffloaderTest {

    final ScheduledExecutorService scheduler;
    final MockBookKeeper bk;
    S3Mock s3mock = null;
    AmazonS3 s3client = null;
    String s3endpoint = null;

    final static String REGION = "foobar";
    final static String BUCKET = "foobar";

    S3ManagedLedgerOffloaderTest() throws Exception {
        scheduler = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("offloader-"));
        bk = new MockBookKeeper(MockedPulsarServiceBaseTest.createMockZooKeeper());
     }

    @BeforeMethod
    public void start() throws Exception {
        s3mock = new S3Mock.Builder().withPort(0).withInMemoryBackend().build();
        int port = s3mock.start().localAddress().getPort();
        s3endpoint = "http://localhost:" + port;

        s3client = AmazonS3ClientBuilder.standard()
            .withRegion(REGION)
            .withEndpointConfiguration(new EndpointConfiguration(s3endpoint, REGION))
            .withPathStyleAccessEnabled(true).build();
        s3client.createBucket(BUCKET);
    }

    @AfterMethod
    public void stop() throws Exception {
        if (s3mock != null) {
            s3mock.shutdown();
        }
    }

    private ReadHandle buildReadHandle() throws Exception {
        LedgerHandle lh = bk.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "foobar".getBytes());
        lh.addEntry("foobar".getBytes());
        lh.close();

        ReadHandle readHandle = bk.newOpenLedgerOp().withLedgerId(lh.getId())
            .withPassword("foobar".getBytes()).withDigestType(DigestType.CRC32).execute().get();
        return lh;
    }

    @Test
    public void testHappyCase() throws Exception {
        LedgerOffloader offloader = new S3ManagedLedgerOffloader(s3client, BUCKET, scheduler);

        offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
    }

    @Test
    public void testBucketDoesNotExist() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setManagedLedgerOffloadDriver(S3ManagedLedgerOffloader.DRIVER_NAME);
        conf.setS3ManagedLedgerOffloadBucket("no-bucket");
        conf.setS3ManagedLedgerOffloadServiceEndpoint(s3endpoint);
        conf.setS3ManagedLedgerOffloadRegion(REGION);
        LedgerOffloader offloader = S3ManagedLedgerOffloader.create(conf, scheduler);

        try {
            offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
            Assert.fail("Shouldn't be able to add to bucket");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getMessage().contains("NoSuchBucket"));
        }
    }

    @Test
    public void testNoRegionConfigured() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setManagedLedgerOffloadDriver(S3ManagedLedgerOffloader.DRIVER_NAME);
        conf.setS3ManagedLedgerOffloadBucket(BUCKET);

        try {
            S3ManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (PulsarServerException pse) {
            // correct
        }
    }

    @Test
    public void testNoBucketConfigured() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setManagedLedgerOffloadDriver(S3ManagedLedgerOffloader.DRIVER_NAME);
        conf.setS3ManagedLedgerOffloadRegion(REGION);

        try {
            S3ManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (PulsarServerException pse) {
            // correct
        }
    }
}

