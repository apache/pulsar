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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Credentials;
import org.jclouds.providers.ProviderMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for tiered storage.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public abstract class JCloudBlobStoreFactory implements Serializable, Cloneable, OffloadDriverMetadataKeys {

    private static final long serialVersionUID = 1L;

    public static final int MB = 1024 * 1024;

    protected static final Logger LOG = LoggerFactory.getLogger(JCloudBlobStoreFactory.class);

    // Maximum number of thread pool threads for ledger off-loading
    protected int managedLedgerOffloadMaxThreads = 2;

    // Max block size in bytes.
    protected int maxBlockSizeInBytes = 64 * MB; // 64MB

    // Read buffer size in bytes.
    protected int readBufferSizeInBytes = MB; // 1MB

    protected Credentials credentials;

    protected JCloudBlobStoreProvider provider;

    public ContextBuilder getContextBuilder() {
        return ContextBuilder.newBuilder(provider.getDriver())
                             .credentials(credentials.identity, credentials.credential)
                             .overrides(getOverrides());
    }

    public BlobStore getBlobStore() {

        LOG.info("Connecting to blobstore : driver: {}, region: {}, bucket: {}, endpoint: {}",
                getProvider().getDriver(), getRegion(), getBucket(), getServiceEndpoint());

        return getContextBuilder()
                .buildView(BlobStoreContext.class)
                .getBlobStore();
    }

    public BlobStoreLocation getBlobStoreLocation() {
        return BlobStoreLocation.of(provider.name(), getRegion(), getBucket(), getServiceEndpoint());
    }

    public Map<String, String> getReadConfig() {
        Map<String, String> metaData = new HashMap<String, String> ();
        metaData.put(METADATA_FIELD_BLOB_STORE_PROVIDER, provider.name());
        metaData.put(METADATA_FIELD_BUCKET, getBucket());
        metaData.put(METADATA_FIELD_REGION, getRegion());
        metaData.put(METADATA_FIELD_ENDPOINT, getServiceEndpoint());
        return metaData;
    }

    protected Properties getOverrides() {
        Properties overrides = new Properties();
        // This property controls the number of parts being uploaded in parallel.
        overrides.setProperty("jclouds.mpu.parallel.degree", "1");
        overrides.setProperty("jclouds.mpu.parts.size", Integer.toString(maxBlockSizeInBytes));
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));
        return overrides;
    }
    
    public String getServiceEndpoint() {
        return null;
    }
    
    public void setServiceEndpoint(String s) {
        // No-op
    }
    
    public abstract String getRegion();
    
    public abstract void setRegion(String s);
    
    public abstract String getBucket();
    
    public abstract void validate();

    public abstract ProviderMetadata getProviderMetadata();

}