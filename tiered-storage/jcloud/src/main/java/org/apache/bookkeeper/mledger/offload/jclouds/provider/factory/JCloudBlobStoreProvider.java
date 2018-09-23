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

import java.io.Serializable;

/**
 * Enumeration of support JCloud Blob Store Providers.
 */
public enum JCloudBlobStoreProvider implements Serializable {

    AWS_S3("aws-s3", S3BlobStoreFactory.class),
    AWS_GLACIER("glacier", GlacierBlobStoreProvider.class),
    AZURE_BLOB("azureblob", AzureBlobStoreFactory.class),
    GOOGLE_CLOUD_STORAGE("google-cloud-storage", GcsBlobStoreFactory.class),
    TRANSIENT("transient", TransientBlobStoreFactory.class);

    public static final boolean driverSupported(String driverName) {
        for (JCloudBlobStoreProvider provider: JCloudBlobStoreProvider.values()) {
            if (provider.getDriver().equalsIgnoreCase(driverName)) {
                return true;
            }
        }
        return false;
    }

    private String driver;
    private Class<? extends JCloudBlobStoreFactory> clazz;

    JCloudBlobStoreProvider(String s, Class<? extends JCloudBlobStoreFactory> clazz) {
        this.driver = s;
        this.clazz = clazz;
    }

    public String getDriver() {
        return driver;
    }

    public Class<? extends JCloudBlobStoreFactory> getClazz() {
        return clazz;
    }
}
