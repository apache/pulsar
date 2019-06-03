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

import static org.apache.pulsar.common.util.FieldParser.value;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;
import lombok.Data;

/**
 * Configuration for tiered storage.
 */
@Data
public class TieredStorageConfigurationData implements Serializable, Cloneable {

    /**** --- Ledger Offloading --- ****/
    // Driver to use to offload old data to long term storage
    private String managedLedgerOffloadDriver = null;

    // Maximum number of thread pool threads for ledger offloading
    private int managedLedgerOffloadMaxThreads = 2;

    // For Amazon S3 ledger offload, AWS region
    private String s3ManagedLedgerOffloadRegion = null;

    // For Amazon S3 ledger offload, Bucket to place offloaded ledger into
    private String s3ManagedLedgerOffloadBucket = null;

    // For Amazon S3 ledger offload, Alternative endpoint to connect to (useful for testing)
    private String s3ManagedLedgerOffloadServiceEndpoint = null;

    // For Amazon S3 ledger offload, Max block size in bytes.
    private int s3ManagedLedgerOffloadMaxBlockSizeInBytes = 64 * 1024 * 1024; // 64MB

    // For Amazon S3 ledger offload, Read buffer size in bytes.
    private int s3ManagedLedgerOffloadReadBufferSizeInBytes = 1024 * 1024; // 1MB

    // For Amazon S3 ledger offload, provide a role to assume before writing to s3
    private String s3ManagedLedgerOffloadRole = null;

    // For Amazon S3 ledger offload, provide a role session name when using a role
    private String s3ManagedLedgerOffloadRoleSessionName = "pulsar-s3-offload";

    // For Google Cloud Storage ledger offload, region where offload bucket is located.
    // reference this page for more details: https://cloud.google.com/storage/docs/bucket-locations
    private String gcsManagedLedgerOffloadRegion = null;

    // For Google Cloud Storage ledger offload, Bucket to place offloaded ledger into
    private String gcsManagedLedgerOffloadBucket = null;

    // For Google Cloud Storage ledger offload, Max block size in bytes.
    private int gcsManagedLedgerOffloadMaxBlockSizeInBytes = 64 * 1024 * 1024; // 64MB

    // For Google Cloud Storage ledger offload, Read buffer size in bytes.
    private int gcsManagedLedgerOffloadReadBufferSizeInBytes = 1024 * 1024; // 1MB

    // For Google Cloud Storage, path to json file containing service account credentials.
    // For more details, see the "Service Accounts" section of https://support.google.com/googleapi/answer/6158849
    private String gcsManagedLedgerOffloadServiceAccountKeyFile = null;

    /**
     * Builds an AWS credential provider based on the offload options
     * @return aws credential provider
     */
    public AWSCredentialsProvider getAWSCredentialProvider() {
        if (Strings.isNullOrEmpty(this.getS3ManagedLedgerOffloadRole())) {
            return DefaultAWSCredentialsProviderChain.getInstance();
        } else {
            String roleName = this.getS3ManagedLedgerOffloadRole();
            String roleSessionName = this.getS3ManagedLedgerOffloadRoleSessionName();
            return new STSAssumeRoleSessionCredentialsProvider.Builder(roleName, roleSessionName).build();
        }
    }

    /**
     * Create a tiered storage configuration from the provided <tt>properties</tt>.
     *
     * @param properties the configuration properties
     * @return tiered storage configuration
     */
    public static TieredStorageConfigurationData create(Properties properties) {
        TieredStorageConfigurationData data = new TieredStorageConfigurationData();
        Field[] fields = TieredStorageConfigurationData.class.getDeclaredFields();
        Arrays.stream(fields).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    f.set(data, value((String) properties.get(f.getName()), f));
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format("failed to initialize %s field while setting value %s",
                            f.getName(), properties.get(f.getName())), e);
                }
            }
        });
        return data;
    }

}
