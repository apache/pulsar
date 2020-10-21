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
package org.apache.pulsar.common.policies.data;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Offload policies configuration test.
 */
public class OffloadPoliciesTest {

    private final int M = 1024 * 1024;
    private final long MIN = 1000 * 60;

    private final String offloadersDirectory = "./test-offloader-directory";
    private final int managedLedgerOffloadMaxThreads = 10;
    private final int managedLedgerOffloadPrefetchRounds = 5;
    private final long offloadThresholdInBytes = 0;
    private final Long offloadDeletionLagInMillis = 5 * MIN;

    @Test
    public void testS3Configuration() {
        final String driver = "aws-s3";
        final String region = "test-region";
        final String bucket = "test-bucket";
        final String endPoint = "test-endpoint";
        final int maxBlockSizeInBytes = 5 * M;
        final int readBufferSizeInBytes = 2 * M;
        final long offloadThresholdInBytes = 10 * M;
        final long offloadDeletionLagInMillis = 5 * MIN;

        OffloadPolicies offloadPolicies = OffloadPolicies.create(
                driver,
                region,
                bucket,
                endPoint,
                maxBlockSizeInBytes,
                readBufferSizeInBytes,
                offloadThresholdInBytes,
                offloadDeletionLagInMillis
        );

        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDriver(), driver);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadRegion(), region);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadBucket(), bucket);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadServiceEndpoint(), endPoint);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadMaxBlockSizeInBytes(), maxBlockSizeInBytes);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadReadBufferSizeInBytes(), readBufferSizeInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(),
                offloadThresholdInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(),
                new Long(offloadDeletionLagInMillis));
    }

    @Test
    public void testGcsConfiguration() {
        final String driver = "google-cloud-storage";
        final String region = "test-region";
        final String bucket = "test-bucket";
        final String endPoint = "test-endpoint";
        final int maxBlockSizeInBytes = 5 * M;
        final int readBufferSizeInBytes = 2 * M;
        final long offloadThresholdInBytes = 0;
        final long offloadDeletionLagInMillis = 5 * MIN;

        OffloadPolicies offloadPolicies = OffloadPolicies.create(
                driver,
                region,
                bucket,
                endPoint,
                maxBlockSizeInBytes,
                readBufferSizeInBytes,
                offloadThresholdInBytes,
                offloadDeletionLagInMillis
        );

        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDriver(), driver);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadRegion(), region);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadBucket(), bucket);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadMaxBlockSizeInBytes(), maxBlockSizeInBytes);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadReadBufferSizeInBytes(), readBufferSizeInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(),
                offloadThresholdInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(),
                new Long(offloadDeletionLagInMillis));
    }

    @Test
    public void testCreateByProperties() {
        final String s3ManagedLedgerOffloadRegion = "test-s3-region";
        final String s3ManagedLedgerOffloadBucket = "test-s3-bucket";
        final String s3ManagedLedgerOffloadServiceEndpoint = "test-s3-endpoint";
        final int s3ManagedLedgerOffloadMaxBlockSizeInBytes = 5 * M;
        final int s3ManagedLedgerOffloadReadBufferSizeInBytes = 2 * M;
        final String s3ManagedLedgerOffloadRole = "test-s3-role";
        final String s3ManagedLedgerOffloadRoleSessionName = "test-s3-role-session-name";

        final String gcsManagedLedgerOffloadRegion = "test-gcs-region";
        final String gcsManagedLedgerOffloadBucket = "test-s3-bucket";
        final int gcsManagedLedgerOffloadMaxBlockSizeInBytes = 10 * M;
        final int gcsManagedLedgerOffloadReadBufferSizeInBytes = 4 * M;
        final String gcsManagedLedgerOffloadServiceAccountKeyFile = "./gcs_key.json";

        final String fileSystemProfilePath = "test-file-system-path";
        final String fileSystemURI = "tset-system-uri";

        final String driver = "test-driver";
        Properties properties = new Properties();

        properties.setProperty("offloadersDirectory", offloadersDirectory);
        properties.setProperty("managedLedgerOffloadDriver", driver);
        properties.setProperty("managedLedgerOffloadMaxThreads", "" + managedLedgerOffloadMaxThreads);
        properties.setProperty("managedLedgerOffloadPrefetchRounds", "" + managedLedgerOffloadPrefetchRounds);
        properties.setProperty("managedLedgerOffloadAutoTriggerSizeThresholdBytes", "" + offloadThresholdInBytes);
        properties.setProperty("managedLedgerOffloadDeletionLagMs", "" + offloadDeletionLagInMillis);

        properties.setProperty("s3ManagedLedgerOffloadRegion", s3ManagedLedgerOffloadRegion);
        properties.setProperty("s3ManagedLedgerOffloadBucket", s3ManagedLedgerOffloadBucket);
        properties.setProperty("s3ManagedLedgerOffloadServiceEndpoint", s3ManagedLedgerOffloadServiceEndpoint);
        properties.setProperty("s3ManagedLedgerOffloadMaxBlockSizeInBytes",
                "" + s3ManagedLedgerOffloadMaxBlockSizeInBytes);
        properties.setProperty("s3ManagedLedgerOffloadReadBufferSizeInBytes",
                "" + s3ManagedLedgerOffloadReadBufferSizeInBytes);
        properties.setProperty("s3ManagedLedgerOffloadRole", s3ManagedLedgerOffloadRole);
        properties.setProperty("s3ManagedLedgerOffloadRoleSessionName", s3ManagedLedgerOffloadRoleSessionName);

        properties.setProperty("gcsManagedLedgerOffloadRegion", gcsManagedLedgerOffloadRegion);
        properties.setProperty("gcsManagedLedgerOffloadBucket", gcsManagedLedgerOffloadBucket);
        properties.setProperty("gcsManagedLedgerOffloadMaxBlockSizeInBytes",
                "" + gcsManagedLedgerOffloadMaxBlockSizeInBytes);
        properties.setProperty("gcsManagedLedgerOffloadReadBufferSizeInBytes",
                "" + gcsManagedLedgerOffloadReadBufferSizeInBytes);
        properties.setProperty("gcsManagedLedgerOffloadServiceAccountKeyFile",
                gcsManagedLedgerOffloadServiceAccountKeyFile);

        properties.setProperty("fileSystemProfilePath", fileSystemProfilePath);
        properties.setProperty("fileSystemURI", fileSystemURI);

        OffloadPolicies offloadPolicies = OffloadPolicies.create(properties);

        Assert.assertEquals(offloadPolicies.getOffloadersDirectory(), offloadersDirectory);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDriver(), driver);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadMaxThreads(), managedLedgerOffloadMaxThreads);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadPrefetchRounds(),
                managedLedgerOffloadPrefetchRounds);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(),
                offloadThresholdInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(),
                new Long(offloadDeletionLagInMillis));

        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadRegion(), s3ManagedLedgerOffloadRegion);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadBucket(), s3ManagedLedgerOffloadBucket);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadServiceEndpoint(),
                s3ManagedLedgerOffloadServiceEndpoint);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadMaxBlockSizeInBytes(),
                s3ManagedLedgerOffloadMaxBlockSizeInBytes);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadReadBufferSizeInBytes(),
                s3ManagedLedgerOffloadReadBufferSizeInBytes);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadRole(), s3ManagedLedgerOffloadRole);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadRoleSessionName(),
                s3ManagedLedgerOffloadRoleSessionName);

        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadRegion(), gcsManagedLedgerOffloadRegion);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadBucket(), gcsManagedLedgerOffloadBucket);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadMaxBlockSizeInBytes(),
                gcsManagedLedgerOffloadMaxBlockSizeInBytes);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadReadBufferSizeInBytes(),
                gcsManagedLedgerOffloadReadBufferSizeInBytes);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadServiceAccountKeyFile(),
                gcsManagedLedgerOffloadServiceAccountKeyFile);

        Assert.assertEquals(offloadPolicies.getFileSystemProfilePath(), fileSystemProfilePath);
        Assert.assertEquals(offloadPolicies.getFileSystemURI(), fileSystemURI);
    }

    @Test
    public void compatibleWithConfigFileTest() {
        Properties properties = new Properties();
        properties.setProperty("managedLedgerOffloadAutoTriggerSizeThresholdBytes", "" + offloadThresholdInBytes);
        properties.setProperty("managedLedgerOffloadDeletionLagMs", "" + offloadDeletionLagInMillis);

        OffloadPolicies offloadPolicies = OffloadPolicies.create(properties);
        Assert.assertEquals(offloadThresholdInBytes, offloadPolicies.getManagedLedgerOffloadThresholdInBytes());
        Assert.assertEquals(offloadDeletionLagInMillis, offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis());

        properties = new Properties();
        properties.setProperty("managedLedgerOffloadThresholdInBytes", "" + (offloadThresholdInBytes + 10));
        properties.setProperty("managedLedgerOffloadDeletionLagInMillis", "" + (offloadDeletionLagInMillis + 10));

        offloadPolicies = OffloadPolicies.create(properties);
        Assert.assertEquals(offloadThresholdInBytes + 10, offloadPolicies.getManagedLedgerOffloadThresholdInBytes());
        Assert.assertEquals(offloadDeletionLagInMillis + 10,
                offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis().longValue());

        properties = new Properties();
        properties.setProperty("managedLedgerOffloadThresholdInBytes", "" + (offloadThresholdInBytes + 20));
        properties.setProperty("managedLedgerOffloadDeletionLagInMillis", "" + (offloadDeletionLagInMillis + 20));
        properties.setProperty("managedLedgerOffloadAutoTriggerSizeThresholdBytes", "" + offloadThresholdInBytes + 30);
        properties.setProperty("managedLedgerOffloadDeletionLagMs", "" + offloadDeletionLagInMillis + 30);

        offloadPolicies = OffloadPolicies.create(properties);
        Assert.assertEquals(offloadThresholdInBytes + 20, offloadPolicies.getManagedLedgerOffloadThresholdInBytes());
        Assert.assertEquals(offloadDeletionLagInMillis + 20,
                offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis().longValue());
    }

}
