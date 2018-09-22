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

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.jclouds.azureblob.AzureBlobProviderMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.providers.ProviderMetadata;

/**
 * Configuration for Azure Blob storage.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class AzureBlobStoreFactory extends JCloudBlobStoreFactory {

    private static final long serialVersionUID = 1L;
    
    private String azureManagedLedgerOffloadRegion = null;

    // For Microsoft Azure Blob Storage ledger offload, the storage account name
    private String azureStorageAccountName = null;

    // For Microsoft Azure Blob Storage ledger offload, the storage account access key
    private String azureStorageAccountKey = null;
    
    // For Azure ledger offload, Container to place offloaded ledger into
    private String azureManagedLedgerOffloadBucket = null;

    @Override
    public String getRegion() {
         return azureManagedLedgerOffloadRegion;
    }

    @Override
    public String getBucket() {
         return azureManagedLedgerOffloadBucket;
    }
    
    @Override
    public void setRegion(String s) {
        azureManagedLedgerOffloadRegion = s;
    }
    
    @Override
    public void validate() {
        if (Strings.isNullOrEmpty(getRegion())) {
            throw new IllegalArgumentException(
                    "Region must be set if Azure Blob Storage offload is enabled");
        }

        if (Strings.isNullOrEmpty(getBucket())) {
            throw new IllegalArgumentException(
                "Bucket cannot be empty for Azure Blob Storage offload");
        }

        if (Strings.isNullOrEmpty(azureStorageAccountName) || Strings.isNullOrEmpty(azureStorageAccountKey)) {
            throw new IllegalArgumentException("Both azureStorageAccountName and azureStorageAccountKey are "
                    + "required if Azure Blob Storage offload is enabled");
        }
    }

    @Override
    public Credentials getCredentials() {
        if (credentials == null) {
            credentials = new Credentials(azureStorageAccountName, azureStorageAccountKey);
        }
        return credentials;
    }

    @Override
    public ProviderMetadata getProviderMetadata() {
        return new AzureBlobProviderMetadata();
    }
}
