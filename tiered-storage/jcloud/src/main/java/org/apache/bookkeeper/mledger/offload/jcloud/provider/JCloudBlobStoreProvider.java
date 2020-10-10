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
package org.apache.bookkeeper.mledger.offload.jcloud.provider;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Strings;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.BlobStoreBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.ConfigValidation;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.CredentialBuilder;

import org.apache.commons.lang3.StringUtils;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.TransientApiMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.googlecloudstorage.GoogleCloudStorageProviderMetadata;
import org.jclouds.osgi.ApiRegistry;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.providers.AnonymousProviderMetadata;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.s3.S3ApiMetadata;
import org.jclouds.s3.reference.S3Constants;

/**
 * Enumeration of the supported JCloud Blob Store Providers.
 * <p>
 * Each Enumeration is responsible for implementation of its own validation,
 * service authentication, and factory method for creating and instance of the
 * JClod BlobStore type.
 *
 * Additional enumerations can be added in the future support other JCloud Providers,
 * currently JClouds supports the following:
 *
 *   - providers=[aws-s3, azureblob, b2, google-cloud-storage, rackspace-cloudfiles-us, rackspace-cloudfiles-uk]
 *   - apis=[s3, sts, transient, atmos, openstack-swift, openstack-keystone, openstack-keystone-3,
 *           rackspace-cloudfiles, rackspace-cloudidentity, filesystem]
 *
 * Note: The driver name associated with each Enum MUST match one of the above vaules, as it is used to instantiate the
 * org.jclouds.ContextBuilder used to create the BlobStore.
 *</p>
 */
@Slf4j
public enum JCloudBlobStoreProvider implements Serializable, ConfigValidation, BlobStoreBuilder, CredentialBuilder  {

    AWS_S3("aws-s3", new AWSS3ProviderMetadata()) {
        @Override
        public void validate(TieredStorageConfiguration config) throws IllegalArgumentException {
            VALIDATION.validate(config);
        }

        @Override
        public BlobStore getBlobStore(TieredStorageConfiguration config) {
            return BLOB_STORE_BUILDER.getBlobStore(config);
        }

        @Override
        public void buildCredentials(TieredStorageConfiguration config) {
            AWS_CREDENTIAL_BUILDER.buildCredentials(config);
        }
    },

    GOOGLE_CLOUD_STORAGE("google-cloud-storage", new GoogleCloudStorageProviderMetadata()) {
        @Override
        public void validate(TieredStorageConfiguration config) throws IllegalArgumentException {
            VALIDATION.validate(config);
        }

        @Override
        public BlobStore getBlobStore(TieredStorageConfiguration config) {
            return BLOB_STORE_BUILDER.getBlobStore(config);
        }

        @Override
        public void buildCredentials(TieredStorageConfiguration config) {
            if (config.getCredentials() == null) {
                try {
                    String gcsKeyContent = Files.toString(
                            new File(config.getConfigProperty("gcsManagedLedgerOffloadServiceAccountKeyFile")),
                                     Charset.defaultCharset());
                    config.setProviderCredentials(new GoogleCredentialsFromJson(gcsKeyContent).get());
                } catch (IOException ioe) {
                    log.error("Cannot read GCS service account credentials file: {}",
                            config.getConfigProperty("gcsManagedLedgerOffloadServiceAccountKeyFile"));
                    throw new IllegalArgumentException(ioe);
                }
            }
        }
    },

    TRANSIENT("transient", new AnonymousProviderMetadata(new TransientApiMetadata(), "")) {
        @Override
        public void validate(TieredStorageConfiguration config) throws IllegalArgumentException {
            if (Strings.isNullOrEmpty(config.getBucket())) {
                throw new IllegalArgumentException(
                    "Bucket cannot be empty for Local offload");
            }
        }

        @Override
        public BlobStore getBlobStore(TieredStorageConfiguration config) {

            ContextBuilder builder =  ContextBuilder.newBuilder("transient");
            BlobStoreContext ctx = builder
                    .buildView(BlobStoreContext.class);
            
            BlobStore bs = ctx.getBlobStore();

            if (!bs.containerExists(config.getBucket())) {
                Location loc = new LocationBuilder()
                        .scope(LocationScope.HOST)
                        .id(UUID.randomUUID() + "")
                        .description("Transient " + config.getBucket())
                        .build();

                bs.createContainerInLocation(loc, config.getBucket());
            }
            System.out.println("Returning " + bs);
            return bs;
        }

        @Override
        public void buildCredentials(TieredStorageConfiguration config) {
            // No-op
        }
    };

    public static JCloudBlobStoreProvider getProvider(String driver) {
        if (StringUtils.isEmpty(driver)) {
            return null;
        }
        for (JCloudBlobStoreProvider provider : JCloudBlobStoreProvider.values()) {
            if (provider.driver.equalsIgnoreCase(driver)) {
                return provider;
            }
        }
        return null;
    }

    public static final boolean driverSupported(String driverName) {
        for (JCloudBlobStoreProvider provider: JCloudBlobStoreProvider.values()) {
            if (provider.getDriver().equalsIgnoreCase(driverName)) {
                return true;
            }
        }
        return false;
    }

    private String driver;
    private ProviderMetadata providerMetadata;

    JCloudBlobStoreProvider(String s, ProviderMetadata providerMetadata) {
        this.driver = s;
        this.providerMetadata = providerMetadata;
    }

    public String getDriver() {
        return driver;
    }

    public ProviderMetadata getProviderMetadata() {
        return providerMetadata;
    }

    // Constants for reuse across AWS, GCS, and Azure, etc.
    static final ConfigValidation VALIDATION = (TieredStorageConfiguration config) -> {
        if (Strings.isNullOrEmpty(config.getRegion()) && Strings.isNullOrEmpty(config.getServiceEndpoint())) {
            throw new IllegalArgumentException(
                "Either Region or ServiceEndpoint must specified for " + config.getDriver() + " offload");
        }

        if (Strings.isNullOrEmpty(config.getBucket())) {
            throw new IllegalArgumentException(
                "Bucket cannot be empty for " + config.getDriver() + " offload");
        }

        if (config.getMaxBlockSizeInBytes() < (5 * 1024 * 1024)) {
            throw new IllegalArgumentException(
                "ManagedLedgerOffloadMaxBlockSizeInBytes cannot be less than 5MB for "
                + config.getDriver() + " offload");
        }
    };

    static final BlobStoreBuilder BLOB_STORE_BUILDER = (TieredStorageConfiguration config) -> {

        Properties overrides = new Properties();
        // This property controls the number of parts being uploaded in parallel.
        overrides.setProperty("jclouds.mpu.parallel.degree", "1");
        overrides.setProperty("jclouds.mpu.parts.size", Integer.toString(config.getMaxBlockSizeInBytes()));
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        if (config.getProvider().equals(AWS_S3)) {
            ApiRegistry.registerApi(new S3ApiMetadata());
            ProviderRegistry.registerProvider(new AWSS3ProviderMetadata());
        } else if (config.getProvider().equals(GOOGLE_CLOUD_STORAGE)) {
            ProviderRegistry.registerProvider(new GoogleCloudStorageProviderMetadata());
        }

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(config.getProviderMetadata());
        contextBuilder.overrides(overrides);

        if (StringUtils.isNotEmpty(config.getServiceEndpoint())) {
            contextBuilder.endpoint(config.getServiceEndpoint());
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }

        if (config.getProviderCredentials() != null) {
                return contextBuilder
                        .credentials(config.getProviderCredentials().identity,
                                     config.getProviderCredentials().credential)
                        .overrides(config.getOverrides())
                        .buildView(BlobStoreContext.class)
                        .getBlobStore();
            } else {
                return contextBuilder
                        .overrides(config.getOverrides())
                        .buildView(BlobStoreContext.class)
                        .getBlobStore();
            }

    };

    static final CredentialBuilder AWS_CREDENTIAL_BUILDER = (TieredStorageConfiguration config) -> {
        if (config.getCredentials() == null) {
            AWSCredentials awsCredentials = null;
            try {
                DefaultAWSCredentialsProviderChain creds = DefaultAWSCredentialsProviderChain.getInstance();
                awsCredentials = creds.getCredentials();
            } catch (Exception e) {
                // allowed, some mock s3 service do not need credential
                log.warn("Exception when get credentials for s3 ", e);
            }
            if (awsCredentials != null) {
                config.setProviderCredentials(
                        new Credentials(awsCredentials.getAWSAccessKeyId(),
                                        awsCredentials.getAWSSecretKey()));
            }
        }
    };
}