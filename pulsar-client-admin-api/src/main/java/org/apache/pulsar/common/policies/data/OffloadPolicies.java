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

import org.apache.pulsar.client.admin.utils.ReflectionUtils;

public interface OffloadPolicies {
    String getOffloadersDirectory();

    String getManagedLedgerOffloadDriver();

    Integer getManagedLedgerOffloadMaxThreads();

    Integer getManagedLedgerOffloadPrefetchRounds();

    Long getManagedLedgerOffloadThresholdInBytes();

    Long getManagedLedgerOffloadDeletionLagInMillis();

    OffloadedReadPriority getManagedLedgerOffloadedReadPriority();

    String getS3ManagedLedgerOffloadRegion();

    String getS3ManagedLedgerOffloadBucket();

    String getS3ManagedLedgerOffloadServiceEndpoint();

    Integer getS3ManagedLedgerOffloadMaxBlockSizeInBytes();

    Integer getS3ManagedLedgerOffloadReadBufferSizeInBytes();

    String getS3ManagedLedgerOffloadCredentialId();

    String getS3ManagedLedgerOffloadCredentialSecret();

    String getS3ManagedLedgerOffloadRole();

    String getS3ManagedLedgerOffloadRoleSessionName();

    String getGcsManagedLedgerOffloadRegion();

    String getGcsManagedLedgerOffloadBucket();

    Integer getGcsManagedLedgerOffloadMaxBlockSizeInBytes();

    Integer getGcsManagedLedgerOffloadReadBufferSizeInBytes();

    String getGcsManagedLedgerOffloadServiceAccountKeyFile();

    String getFileSystemProfilePath();

    String getFileSystemURI();

    String getManagedLedgerOffloadBucket();

    String getManagedLedgerOffloadRegion();

    String getManagedLedgerOffloadServiceEndpoint();

    Integer getManagedLedgerOffloadMaxBlockSizeInBytes();

    Integer getManagedLedgerOffloadReadBufferSizeInBytes();

    interface Builder {

        Builder offloadersDirectory(String offloadersDirectory);

        Builder managedLedgerOffloadDriver(String managedLedgerOffloadDriver);

        Builder managedLedgerOffloadMaxThreads(Integer managedLedgerOffloadMaxThreads);

        Builder managedLedgerOffloadPrefetchRounds(Integer managedLedgerOffloadPrefetchRounds);

        Builder managedLedgerOffloadThresholdInBytes(Long managedLedgerOffloadThresholdInBytes);

        Builder managedLedgerOffloadDeletionLagInMillis(Long managedLedgerOffloadDeletionLagInMillis);

        Builder managedLedgerOffloadedReadPriority(OffloadedReadPriority managedLedgerOffloadedReadPriority);

        Builder s3ManagedLedgerOffloadRegion(String s3ManagedLedgerOffloadRegion);

        Builder s3ManagedLedgerOffloadBucket(String s3ManagedLedgerOffloadBucket);

        Builder s3ManagedLedgerOffloadServiceEndpoint(String s3ManagedLedgerOffloadServiceEndpoint);

        Builder s3ManagedLedgerOffloadMaxBlockSizeInBytes(Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes);

        Builder s3ManagedLedgerOffloadReadBufferSizeInBytes(Integer s3ManagedLedgerOffloadReadBufferSizeInBytes);

        Builder s3ManagedLedgerOffloadCredentialId(String s3ManagedLedgerOffloadCredentialId);

        Builder s3ManagedLedgerOffloadCredentialSecret(String s3ManagedLedgerOffloadCredentialSecret);

        Builder s3ManagedLedgerOffloadRole(String s3ManagedLedgerOffloadRole);

        Builder setS3ManagedLedgerOffloadRoleSessionName(String s3ManagedLedgerOffloadRoleSessionName);

        Builder gcsManagedLedgerOffloadRegion(String gcsManagedLedgerOffloadRegion);

        Builder gcsManagedLedgerOffloadBucket(String gcsManagedLedgerOffloadBucket);

        Builder gcsManagedLedgerOffloadMaxBlockSizeInBytes(Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes);

        Builder gcsManagedLedgerOffloadReadBufferSizeInBytes(Integer gcsManagedLedgerOffloadReadBufferSizeInBytes);

        Builder gcsManagedLedgerOffloadServiceAccountKeyFile(String gcsManagedLedgerOffloadServiceAccountKeyFile);

        Builder fileSystemProfilePath(String fileSystemProfilePath);

        Builder fileSystemURI(String fileSystemURI);

        Builder managedLedgerOffloadBucket(String managedLedgerOffloadBucket);

        Builder managedLedgerOffloadRegion(String managedLedgerOffloadRegion);

        Builder managedLedgerOffloadServiceEndpoint(String managedLedgerOffloadServiceEndpoint);

        Builder managedLedgerOffloadMaxBlockSizeInBytes(Integer managedLedgerOffloadMaxBlockSizeInBytes);

        Builder managedLedgerOffloadReadBufferSizeInBytes(Integer managedLedgerOffloadReadBufferSizeInBytes);

        OffloadPolicies build();
    }

    static Builder builder() {
        return ReflectionUtils.newBuilder("org.apache.pulsar.common.policies.data.OffloadPoliciesImpl");
    }
}
