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

public interface OffloadPoliciesInterface {
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

    void setOffloadersDirectory(String offloadersDirectory);

    void setManagedLedgerOffloadDriver(String managedLedgerOffloadDriver);

    void setManagedLedgerOffloadMaxThreads(Integer managedLedgerOffloadMaxThreads);

    void setManagedLedgerOffloadPrefetchRounds(Integer managedLedgerOffloadPrefetchRounds);

    void setManagedLedgerOffloadThresholdInBytes(Long managedLedgerOffloadThresholdInBytes);

    void setManagedLedgerOffloadDeletionLagInMillis(Long managedLedgerOffloadDeletionLagInMillis);

    void setManagedLedgerOffloadedReadPriority(OffloadedReadPriority managedLedgerOffloadedReadPriority);

    void setS3ManagedLedgerOffloadRegion(String s3ManagedLedgerOffloadRegion);

    void setS3ManagedLedgerOffloadBucket(String s3ManagedLedgerOffloadBucket);

    void setS3ManagedLedgerOffloadServiceEndpoint(String s3ManagedLedgerOffloadServiceEndpoint);

    void setS3ManagedLedgerOffloadMaxBlockSizeInBytes(Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes);

    void setS3ManagedLedgerOffloadReadBufferSizeInBytes(Integer s3ManagedLedgerOffloadReadBufferSizeInBytes);

    void setS3ManagedLedgerOffloadCredentialId(String s3ManagedLedgerOffloadCredentialId);

    void setS3ManagedLedgerOffloadCredentialSecret(String s3ManagedLedgerOffloadCredentialSecret);

    void setS3ManagedLedgerOffloadRole(String s3ManagedLedgerOffloadRole);

    void setS3ManagedLedgerOffloadRoleSessionName(String s3ManagedLedgerOffloadRoleSessionName);

    void setGcsManagedLedgerOffloadRegion(String gcsManagedLedgerOffloadRegion);

    void setGcsManagedLedgerOffloadBucket(String gcsManagedLedgerOffloadBucket);

    void setGcsManagedLedgerOffloadMaxBlockSizeInBytes(Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes);

    void setGcsManagedLedgerOffloadReadBufferSizeInBytes(Integer gcsManagedLedgerOffloadReadBufferSizeInBytes);

    void setGcsManagedLedgerOffloadServiceAccountKeyFile(String gcsManagedLedgerOffloadServiceAccountKeyFile);

    void setFileSystemProfilePath(String fileSystemProfilePath);

    void setFileSystemURI(String fileSystemURI);

    void setManagedLedgerOffloadBucket(String managedLedgerOffloadBucket);

    void setManagedLedgerOffloadRegion(String managedLedgerOffloadRegion);

    void setManagedLedgerOffloadServiceEndpoint(String managedLedgerOffloadServiceEndpoint);

    void setManagedLedgerOffloadMaxBlockSizeInBytes(Integer managedLedgerOffloadMaxBlockSizeInBytes);

    void setManagedLedgerOffloadReadBufferSizeInBytes(Integer managedLedgerOffloadReadBufferSizeInBytes);
}
