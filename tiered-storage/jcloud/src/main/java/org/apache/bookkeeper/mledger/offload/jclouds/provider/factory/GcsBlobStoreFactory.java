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

import com.google.common.base.Strings;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.jclouds.domain.Credentials;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.googlecloudstorage.GoogleCloudStorageProviderMetadata;
import org.jclouds.providers.ProviderMetadata;

/**
 * Configuration for Google Cloud storage.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class GcsBlobStoreFactory extends JCloudBlobStoreFactory {

    private static final long serialVersionUID = 1L;

    // For Google Cloud Storage, path to json file containing service account credentials.
    // For more details, see the "Service Accounts" section of https://support.google.com/googleapi/answer/6158849
    private String gcsManagedLedgerOffloadServiceAccountKeyFile = null;
    
    private String gcsManagedLedgerOffloadRegion = null;
    
    private String gcsManagedLedgerOffloadBucket = null;
    
    @Override
    public String getRegion() {
        return gcsManagedLedgerOffloadRegion;
    }

    @Override
    public String getBucket() {
        return gcsManagedLedgerOffloadBucket;
    }
    
    @Override
    public void setRegion(String s) {
        gcsManagedLedgerOffloadRegion = s; 
    }

    @Override
    public void validate() {
        if (Strings.isNullOrEmpty(getRegion())) {
            throw new IllegalArgumentException(
                    "gcsManagedLedgerOffloadRegion must be set if Google Cloud Storage offload is enabled");
        }

        if (Strings.isNullOrEmpty(getBucket())) {
            throw new IllegalArgumentException(
                "gcsManagedLedgerOffloadBucket cannot be empty for Google Cloud Storage offload");
        }

        if (maxBlockSizeInBytes < 5 * MB) {
            throw new IllegalArgumentException(
                "maxBlockSizeInBytes cannot be less than 5MB for Google Cloud Storage offload");
        }

        if (Strings.isNullOrEmpty(gcsManagedLedgerOffloadServiceAccountKeyFile)) {
            throw new IllegalArgumentException(
                "The service account key path is empty for GCS driver");
        }
    }

    @Override
    public Credentials getCredentials() {
        if (credentials == null) {
            try {
                String gcsKeyContent = Files.toString(
                        new File(gcsManagedLedgerOffloadServiceAccountKeyFile), Charset.defaultCharset());
                credentials = new GoogleCredentialsFromJson(gcsKeyContent).get();
            } catch (IOException ioe) {
                LOG.error("Cannot read GCS service account credentials file: {}",
                        gcsManagedLedgerOffloadServiceAccountKeyFile);
                throw new IllegalArgumentException(ioe);
            }
        }
        return credentials;
    }

    @Override
    public ProviderMetadata getProviderMetadata() {
        return new GoogleCloudStorageProviderMetadata();
    }
}