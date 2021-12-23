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

import static org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.GCS_ACCOUNT_KEY_FILE_FIELD;
import static org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.S3_ID_FIELD;
import static org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.S3_ROLE_FIELD;
import static org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.S3_ROLE_SESSION_NAME_FIELD;
import static org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration.S3_SECRET_FIELD;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
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
import org.jclouds.ContextBuilder;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.azureblob.AzureBlobProviderMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.TransientApiMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.googlecloudstorage.GoogleCloudStorageProviderMetadata;
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
                    String gcsKeyContent = Files.asCharSource(
                            new File(config.getConfigProperty(GCS_ACCOUNT_KEY_FILE_FIELD)),
                            Charset.defaultCharset()).read();
                    config.setProviderCredentials(() -> new GoogleCredentialsFromJson(gcsKeyContent).get());
                } catch (IOException ioe) {
                    log.error("Cannot read GCS service account credentials file: {}",
                            config.getConfigProperty("gcsManagedLedgerOffloadServiceAccountKeyFile"));
                    throw new IllegalArgumentException(ioe);
                }
            }
        }
    },

    AZURE_BLOB("azureblob", new AzureBlobProviderMetadata()) {
        @Override
        public void validate(TieredStorageConfiguration config) throws IllegalArgumentException {
            VALIDATION.validate(config);
        }

        @Override
        public BlobStore getBlobStore(TieredStorageConfiguration config) {
            ContextBuilder contextBuilder = ContextBuilder.newBuilder(config.getProviderMetadata());
            contextBuilder.overrides(config.getOverrides());

            if (config.getProviderCredentials() != null) {
                Credentials credentials = config.getProviderCredentials().get();
                return contextBuilder
                        .credentials(credentials.identity, credentials.credential)
                        .buildView(BlobStoreContext.class)
                        .getBlobStore();
            } else {
                log.warn("The credentials is null. driver: {}, bucket: {}", config.getDriver(), config.getBucket());
                return contextBuilder
                        .buildView(BlobStoreContext.class)
                        .getBlobStore();
            }
        }

        @Override
        public void buildCredentials(TieredStorageConfiguration config) {
            String accountName = System.getenv("AZURE_STORAGE_ACCOUNT");
            if (StringUtils.isEmpty(accountName)) {
                throw new IllegalArgumentException("Couldn't get the azure storage account.");
            }
            String accountKey = System.getenv("AZURE_STORAGE_ACCESS_KEY");
            if (StringUtils.isEmpty(accountKey)) {
                throw new IllegalArgumentException("Couldn't get the azure storage access key.");
            }
            config.setProviderCredentials(() -> new Credentials(accountName, accountKey));
        }
    },


    /**
     * Aliyun OSS is compatible with the S3 API
     * https://www.alibabacloud.com/help/doc-detail/64919.htm
     */
    ALIYUN_OSS("aliyun-oss", new AnonymousProviderMetadata(new S3ApiMetadata(), "")) {
        @Override
        public void validate(TieredStorageConfiguration config) throws IllegalArgumentException {
            ALIYUN_OSS_VALIDATION.validate(config);
        }

        @Override
        public BlobStore getBlobStore(TieredStorageConfiguration config) {
            return ALIYUN_OSS_BLOB_STORE_BUILDER.getBlobStore(config);
        }

        @Override
        public void buildCredentials(TieredStorageConfiguration config) {
            ALIYUN_OSS_CREDENTIAL_BUILDER.buildCredentials(config);
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
        ContextBuilder contextBuilder = ContextBuilder.newBuilder(config.getProviderMetadata());
        contextBuilder.overrides(config.getOverrides());

        if (StringUtils.isNotEmpty(config.getServiceEndpoint())) {
            contextBuilder.endpoint(config.getServiceEndpoint());
        }

        if (config.getProviderCredentials() != null) {
                return contextBuilder
                        .credentialsSupplier(config.getCredentials()::get)
                        .buildView(BlobStoreContext.class)
                        .getBlobStore();
        } else {
            log.warn("The credentials is null. driver: {}, bucket: {}", config.getDriver(), config.getBucket());
            return contextBuilder
                    .buildView(BlobStoreContext.class)
                    .getBlobStore();
        }

    };

    static final CredentialBuilder AWS_CREDENTIAL_BUILDER = (TieredStorageConfiguration config) -> {
        if (config.getCredentials() == null) {
            final AWSCredentialsProvider authChain;
            try {
                if (!Strings.isNullOrEmpty(config.getConfigProperty(S3_ID_FIELD))
                    && !Strings.isNullOrEmpty(config.getConfigProperty(S3_SECRET_FIELD))) {
                    AWSCredentials awsCredentials = new AWSCredentials() {
                        @Override
                        public String getAWSAccessKeyId() {
                            return config.getConfigProperty(S3_ID_FIELD);
                        }

                        @Override
                        public String getAWSSecretKey() {
                            return config.getConfigProperty(S3_SECRET_FIELD);
                        }
                    };
                    authChain = new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(
                                config.getConfigProperty(S3_ID_FIELD),
                                config.getConfigProperty(S3_SECRET_FIELD)));
                } else if (Strings.isNullOrEmpty(config.getConfigProperty(S3_ROLE_FIELD))) {
                    authChain = DefaultAWSCredentialsProviderChain.getInstance();
                } else {
                    authChain =
                            new STSAssumeRoleSessionCredentialsProvider.Builder(
                                    config.getConfigProperty(S3_ROLE_FIELD),
                                    config.getConfigProperty(S3_ROLE_SESSION_NAME_FIELD)
                            ).build();
                }

                // Important! Delay the building of actual credentials
                // until later to support tokens that may be refreshed
                // such as all session tokens
                config.setProviderCredentials(() -> {
                    AWSCredentials newCreds = authChain.getCredentials();
                    Credentials jcloudCred = null;

                    if (newCreds instanceof AWSSessionCredentials) {
                        // if we have session credentials, we need to send the session token
                        // this allows us to support EC2 metadata credentials
                        jcloudCred = SessionCredentials.builder()
                                .accessKeyId(newCreds.getAWSAccessKeyId())
                                .secretAccessKey(newCreds.getAWSSecretKey())
                                .sessionToken(((AWSSessionCredentials) newCreds).getSessionToken())
                                .build();
                    } else {
                        // in the event we hit this branch, we likely don't have expiring
                        // credentials, however, this still allows for the user to update
                        // profiles creds or some other mechanism
                        jcloudCred = new Credentials(
                                newCreds.getAWSAccessKeyId(), newCreds.getAWSSecretKey());
                    }
                    return jcloudCred;
                });
            } catch (Exception e) {
                // allowed, some mock s3 service do not need credential
                log.warn("Exception when get credentials for s3 ", e);
            }
        }
    };

    static final BlobStoreBuilder ALIYUN_OSS_BLOB_STORE_BUILDER = (TieredStorageConfiguration config) -> {
        ContextBuilder contextBuilder = ContextBuilder.newBuilder(config.getProviderMetadata());
        Properties overrides = config.getOverrides();
        // For security reasons, OSS supports only virtual hosted style access.
        overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "true");
        contextBuilder.overrides(overrides);
        contextBuilder.endpoint(config.getServiceEndpoint());

        if (config.getProviderCredentials() != null) {
            return contextBuilder
                    .credentialsSupplier(config.getCredentials()::get)
                    .buildView(BlobStoreContext.class)
                    .getBlobStore();
        } else {
            log.warn("The credentials is null. driver: {}, bucket: {}", config.getDriver(), config.getBucket());
            return contextBuilder
                    .buildView(BlobStoreContext.class)
                    .getBlobStore();
        }
    };

    static final ConfigValidation ALIYUN_OSS_VALIDATION = (TieredStorageConfiguration config) -> {
        if (Strings.isNullOrEmpty(config.getServiceEndpoint())) {
            throw new IllegalArgumentException(
                    "ServiceEndpoint must specified for " + config.getDriver() + " offload");
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

    static final CredentialBuilder ALIYUN_OSS_CREDENTIAL_BUILDER = (TieredStorageConfiguration config) -> {
        String accountName = System.getenv("ALIYUN_OSS_ACCESS_KEY_ID");
        if (StringUtils.isEmpty(accountName)) {
            throw new IllegalArgumentException("Couldn't get the aliyun oss access key id.");
        }
        String accountKey = System.getenv("ALIYUN_OSS_ACCESS_KEY_SECRET");
        if (StringUtils.isEmpty(accountKey)) {
            throw new IllegalArgumentException("Couldn't get the aliyun oss access key secret.");
        }
        Credentials credentials = new Credentials(
                accountName, accountKey);
        config.setProviderCredentials(() -> credentials);
    };

}
