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

import java.util.Properties;
import org.apache.pulsar.common.policies.data.OffloadPolicies.OffloadedReadPriority;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Offload policies configuration test.
 */
public class OffloadPoliciesTest {

    private final int M = 1024 * 1024;
    private final long MIN = 1000 * 60;

    private final String offloadersDirectory = "./test-offloader-directory";
    private final Integer managedLedgerOffloadMaxThreads = 10;
    private final Integer managedLedgerOffloadPrefetchRounds = 5;
    private final Long offloadThresholdInBytes = 0L;
    private final Long offloadDeletionLagInMillis = 5 * MIN;

    @Test
    public void testS3Configuration() {
        final String driver = "aws-s3";
        final String region = "test-region";
        final String bucket = "test-bucket";
        final String credentialId = "test-credential-id";
        final String credentialSecret = "test-credential-secret";
        final String endPoint = "test-endpoint";
        final Integer maxBlockSizeInBytes = 5 * M;
        final Integer readBufferSizeInBytes = 2 * M;
        final Long offloadThresholdInBytes = 10L * M;
        final Long offloadDeletionLagInMillis = 5L * MIN;

        OffloadPolicies offloadPolicies = OffloadPolicies.create(
                driver,
                region,
                bucket,
                endPoint,
                credentialId,
                credentialSecret,
                maxBlockSizeInBytes,
                readBufferSizeInBytes,
                offloadThresholdInBytes,
                offloadDeletionLagInMillis,
                OffloadedReadPriority.TIERED_STORAGE_FIRST
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
        final String credentialId = "test-credential-id";
        final String credentialSecret = "test-credential-secret";
        final Integer maxBlockSizeInBytes = 5 * M;
        final Integer readBufferSizeInBytes = 2 * M;
        final Long offloadThresholdInBytes = 0L;
        final Long offloadDeletionLagInMillis = 5 * MIN;
        final OffloadedReadPriority readPriority = OffloadedReadPriority.TIERED_STORAGE_FIRST;

        OffloadPolicies offloadPolicies = OffloadPolicies.create(
                driver,
                region,
                bucket,
                endPoint,
                credentialId,
                credentialSecret,
                maxBlockSizeInBytes,
                readBufferSizeInBytes,
                offloadThresholdInBytes,
                offloadDeletionLagInMillis,
                readPriority
        );

        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDriver(), driver);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadRegion(), region);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadBucket(), bucket);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadMaxBlockSizeInBytes(), maxBlockSizeInBytes);
        Assert.assertEquals(offloadPolicies.getGcsManagedLedgerOffloadReadBufferSizeInBytes(), readBufferSizeInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(), offloadThresholdInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(), offloadDeletionLagInMillis);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadedReadPriority(), readPriority);
    }

    @Test
    public void testCreateByProperties() {
        final String s3ManagedLedgerOffloadRegion = "test-s3-region";
        final String s3ManagedLedgerOffloadBucket = "test-s3-bucket";
        final String s3ManagedLedgerOffloadServiceEndpoint = "test-s3-endpoint";
        final Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes = 5 * M;
        final Integer s3ManagedLedgerOffloadReadBufferSizeInBytes = 2 * M;
        final String s3ManagedLedgerOffloadRole = "test-s3-role";
        final String s3ManagedLedgerOffloadRoleSessionName = "test-s3-role-session-name";

        final String gcsManagedLedgerOffloadRegion = "test-gcs-region";
        final String gcsManagedLedgerOffloadBucket = "test-s3-bucket";
        final Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes = 10 * M;
        final Integer gcsManagedLedgerOffloadReadBufferSizeInBytes = 4 * M;
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
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadPrefetchRounds(), managedLedgerOffloadPrefetchRounds);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(), offloadThresholdInBytes);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(), offloadDeletionLagInMillis);

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
        Assert.assertEquals(Long.valueOf(offloadThresholdInBytes + 10),
                offloadPolicies.getManagedLedgerOffloadThresholdInBytes());
        Assert.assertEquals(offloadDeletionLagInMillis + 10,
                offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis().longValue());

        properties = new Properties();
        properties.setProperty("managedLedgerOffloadThresholdInBytes", "" + (offloadThresholdInBytes + 20));
        properties.setProperty("managedLedgerOffloadDeletionLagInMillis", "" + (offloadDeletionLagInMillis + 20));
        properties.setProperty("managedLedgerOffloadAutoTriggerSizeThresholdBytes", "" + offloadThresholdInBytes + 30);
        properties.setProperty("managedLedgerOffloadDeletionLagMs", "" + offloadDeletionLagInMillis + 30);

        offloadPolicies = OffloadPolicies.create(properties);
        Assert.assertEquals(Long.valueOf(offloadThresholdInBytes + 20),
                offloadPolicies.getManagedLedgerOffloadThresholdInBytes());
        Assert.assertEquals(offloadDeletionLagInMillis + 20,
                offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis().longValue());
    }

    @Test
    public void oldPoliciesCompatibleTest() {
        Policies policies = new Policies();
        Assert.assertEquals(policies.offload_threshold, -1);

        OffloadPolicies offloadPolicies = OffloadPolicies.oldPoliciesCompatible(null, policies);
        Assert.assertNull(offloadPolicies);

        policies.offload_deletion_lag_ms = 1000L;
        policies.offload_threshold = 0;
        offloadPolicies = OffloadPolicies.oldPoliciesCompatible(offloadPolicies, policies);
        Assert.assertNotNull(offloadPolicies);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(), new Long(1000));
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(), new Long(0));

        policies.offload_deletion_lag_ms = 2000L;
        policies.offload_threshold = 100;
        offloadPolicies = OffloadPolicies.oldPoliciesCompatible(offloadPolicies, policies);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(), new Long(1000));
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(), new Long(0));
    }

    @Test
    public void mergeTest() {
        final String topicBucket = "topic-bucket";
        OffloadPolicies topicLevelPolicies = new OffloadPolicies();
        topicLevelPolicies.setS3ManagedLedgerOffloadBucket(topicBucket);

        final String nsBucket = "ns-bucket";
        final Long nsDeletionLag = 2000L;
        OffloadPolicies nsLevelPolicies = new OffloadPolicies();
        nsLevelPolicies.setManagedLedgerOffloadDeletionLagInMillis(nsDeletionLag);
        nsLevelPolicies.setS3ManagedLedgerOffloadBucket(nsBucket);

        final String brokerDriver = "aws-s3";
        final Long brokerOffloadThreshold = 0L;
        final long brokerDeletionLag = 1000L;
        final Integer brokerOffloadMaxThreads = 2;
        Properties brokerProperties = new Properties();
        brokerProperties.setProperty("managedLedgerOffloadDriver", brokerDriver);
        brokerProperties.setProperty("managedLedgerOffloadAutoTriggerSizeThresholdBytes", "" + brokerOffloadThreshold);
        brokerProperties.setProperty("managedLedgerOffloadDeletionLagMs", "" + brokerDeletionLag);
        brokerProperties.setProperty("managedLedgerOffloadMaxThreads", "" + brokerOffloadMaxThreads);

        OffloadPolicies offloadPolicies =
                OffloadPolicies.mergeConfiguration(topicLevelPolicies, nsLevelPolicies, brokerProperties);
        Assert.assertNotNull(offloadPolicies);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDriver(), brokerDriver);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadBucket(), topicBucket);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(), nsDeletionLag);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(), brokerOffloadThreshold);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadMaxThreads(), brokerOffloadMaxThreads);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadPrefetchRounds(),
                new Integer(OffloadPolicies.DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS));
        Assert.assertNull(offloadPolicies.getS3ManagedLedgerOffloadRegion());
    }

}
