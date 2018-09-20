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
package org.apache.bookkeeper.mledger.offload.jcloud.config;

import com.google.common.base.Strings;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.jclouds.domain.Credentials;

/**
 * Configuration for Azure Blob storage.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class AzureTieredStorageConfiguration extends JCloudBlobStoreConfiguration {

    private static final long serialVersionUID = 1L;

    /* For Microsoft Azure Blob Storage ledger offload, region where offload container is located.
     * Reference this page for more details:
     * https://azure.microsoft.com/en-us/global-infrastructure/services/?products=storage&regions=all
     */
    private String azureManagedLedgerOffloadRegion = null;

    // For Microsoft Azure Blob Storage ledger offload, Container name to place offloader ledger into.
    // Analogus to "bucket' in AWS or GCS
    private String azureManagedLedgerOffloadContainer;

    // For Microsoft Azure Blob Storage ledger offload, the storage account name
    private String azureStorageAccountName = null;

    // For Microsoft Azure Blob Storage ledger offload, the storage account access key
    private String azureStorageAccountKey = null;

    @Override
    public String getRegion() {
        return azureManagedLedgerOffloadRegion;
    }

    @Override
    public String getBucket() {
       return azureManagedLedgerOffloadContainer;
    }

    @Override
    public String getServiceEndpoint() {
        return null;
    }

    @Override
    public void validate() {
        if (Strings.isNullOrEmpty(getRegion())) {
            throw new IllegalArgumentException(
                    "azureManagedLedgerOffloadRegion must be set if Azure Blob Storage offload is enabled");
        }

        if (Strings.isNullOrEmpty(getBucket())) {
            throw new IllegalArgumentException(
                "azureManagedLedgerOffloadContainer cannot be empty for Azure Blob Storage offload");
        }

        if (Strings.isNullOrEmpty(azureStorageAccountName) || Strings.isNullOrEmpty(azureStorageAccountKey)) {
            throw new IllegalArgumentException("Both azureStorageAccountName and azureStorageAccountKey are "
                    + "required if Azure Blob Storage offload is enabled");
        }
    }

    @Override
    public Credentials getCredentials() {
        return new Credentials(azureStorageAccountName, azureStorageAccountKey);
    }
}
