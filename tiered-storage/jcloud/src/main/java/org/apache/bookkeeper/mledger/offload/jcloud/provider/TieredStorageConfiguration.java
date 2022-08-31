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

import static org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider.AWS_S3;
import static org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.Constants;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.domain.Credentials;
import org.jclouds.googlecloudstorage.GoogleCloudStorageProviderMetadata;
import org.jclouds.osgi.ApiRegistry;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.s3.S3ApiMetadata;
import org.jclouds.s3.reference.S3Constants;

/**
 * Class responsible for holding all of the tiered storage configuration data
 * that is set in the global Pulsar broker.conf file.
 * <p>
 * This class is used by the BlobStoreManagedLedgerOffloader to determine which
 * JCloud provider to use for Tiered Storage offloand, along with the associated
 * properties such as region, bucket, user credentials, etc.
 * </p>
 */
@Slf4j
public class TieredStorageConfiguration {

    private static final long serialVersionUID = 1L;
    public static final String BLOB_STORE_PROVIDER_KEY = "managedLedgerOffloadDriver";
    public static final String METADATA_FIELD_BUCKET = "bucket";
    public static final String METADATA_FIELD_REGION = "region";
    public static final String METADATA_FIELD_ENDPOINT = "serviceEndpoint";
    public static final String METADATA_FIELD_MAX_BLOCK_SIZE = "maxBlockSizeInBytes";
    public static final String METADATA_FIELD_MIN_BLOCK_SIZE = "minBlockSizeInBytes";
    public static final String METADATA_FIELD_READ_BUFFER_SIZE = "readBufferSizeInBytes";
    public static final String METADATA_FIELD_WRITE_BUFFER_SIZE = "writeBufferSizeInBytes";
    public static final String OFFLOADER_PROPERTY_PREFIX = "managedLedgerOffload";
    public static final String MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC = "maxOffloadSegmentRolloverTimeInSeconds";
    public static final String MIN_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC = "minOffloadSegmentRolloverTimeInSeconds";
    public static final long DEFAULT_MAX_SEGMENT_TIME_IN_SECOND = 600;
    public static final long DEFAULT_MIN_SEGMENT_TIME_IN_SECOND = 0;
    public static final String MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES = "maxOffloadSegmentSizeInBytes";
    public static final long DEFAULT_MAX_SEGMENT_SIZE_IN_BYTES = 1024 * 1024 * 1024;

    protected static final int MB = 1024 * 1024;

    public static final String GCS_ACCOUNT_KEY_FILE_FIELD = "gcsManagedLedgerOffloadServiceAccountKeyFile";
    public static final String S3_ID_FIELD = "s3ManagedLedgerOffloadCredentialId";
    public static final String S3_SECRET_FIELD = "s3ManagedLedgerOffloadCredentialSecret";
    public static final String S3_ROLE_FIELD = "s3ManagedLedgerOffloadRole";
    public static final String S3_ROLE_SESSION_NAME_FIELD = "s3ManagedLedgerOffloadRoleSessionName";

    public static TieredStorageConfiguration create(Properties props) throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        map.putAll(props.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(),
                                          e -> e.getValue().toString())));

        return new TieredStorageConfiguration(map);
    }

    public static TieredStorageConfiguration create(Map<String, String> props) {
        return new TieredStorageConfiguration(props);
    }

    @Getter
    private final Map<String, String> configProperties;
    @Getter
    private Supplier<Credentials> credentials;
    private JCloudBlobStoreProvider provider;

    public TieredStorageConfiguration(Map<String, String> configProperties) {
        if (configProperties != null) {
            this.configProperties = configProperties;
        } else {
            throw new IllegalArgumentException("configProperties cannot be null");
        }
    }

    public List<String> getKeys(String property) {
        List<String> keys = new ArrayList<String> ();

        String bc = getBackwardCompatibleKey(property);
        if (StringUtils.isNotBlank(bc)) {
            keys.add(bc);
        }

        String key = getKeyName(property);
        if (StringUtils.isNotBlank(key)) {
            keys.add(key);
        }
        return keys;
    }

    private String getKeyName(String property) {
        StringBuilder sb = new StringBuilder();
        sb.append(OFFLOADER_PROPERTY_PREFIX)
          .append(StringUtils.capitalize(property));

        return sb.toString();
    }

    private String getBackwardCompatibleKey(String property) {
        switch (getProvider()) {
            case AWS_S3:
                return new StringBuilder().append("s3ManagedLedgerOffload")
                                          .append(StringUtils.capitalize(property))
                                          .toString();

            case GOOGLE_CLOUD_STORAGE:
                return new StringBuilder().append("gcsManagedLedgerOffload")
                                          .append(StringUtils.capitalize(property))
                                          .toString();

            default:
                return null;
        }
    }

    public String getBlobStoreProviderKey() {
        return configProperties.getOrDefault(BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
    }

    public String getDriver() {
        return getProvider().getDriver();
    }

    public String getRegion() {
        for (String key : getKeys(METADATA_FIELD_REGION)) {
            if (configProperties.containsKey(key)) {
                return configProperties.get(key);
            }
        }
        return null;
    }

    public void setRegion(String s) {
        configProperties.put(getKeyName(METADATA_FIELD_REGION), s);
    }

    public String getBucket() {
        for (String key : getKeys(METADATA_FIELD_BUCKET)) {
            if (configProperties.containsKey(key)) {
                return configProperties.get(key);
            }
        }
        return null;
    }

    public String getServiceEndpoint() {
        for (String key : getKeys(METADATA_FIELD_ENDPOINT)) {
            if (configProperties.containsKey(key)) {
                return configProperties.get(key);
            }
        }
        return null;
    }

    public long getMaxSegmentTimeInSecond() {
        if (configProperties.containsKey(MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC)) {
            return Long.parseLong(configProperties.get(MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC));
        } else {
            return DEFAULT_MAX_SEGMENT_TIME_IN_SECOND;
        }
    }

    public long getMinSegmentTimeInSecond() {
        if (configProperties.containsKey(MIN_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC)) {
            return Long.parseLong(configProperties.get(MIN_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC));
        } else {
            return DEFAULT_MIN_SEGMENT_TIME_IN_SECOND;
        }
    }

    public long getMaxSegmentSizeInBytes() {
        if (configProperties.containsKey(MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES)) {
            return Long.parseLong(configProperties.get(MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES));
        } else {
            return DEFAULT_MAX_SEGMENT_SIZE_IN_BYTES;
        }
    }

    public void setServiceEndpoint(String s) {
        configProperties.put(getKeyName(METADATA_FIELD_ENDPOINT), s);
    }

    /**
     * Used to find a specific configuration property other than
     * one of the predefined ones. This allows for any number of
     * provider specific, or new properties to added in the future.
     *
     * @param propertyName
     * @return
     */
    public String getConfigProperty(String propertyName) {
        return configProperties.get(propertyName);
    }

    public JCloudBlobStoreProvider getProvider() {
        if (provider == null) {
            provider = JCloudBlobStoreProvider.getProvider(getBlobStoreProviderKey());
        }
        return provider;
    }

    public void setProvider(JCloudBlobStoreProvider provider) {
        this.provider = provider;
    }

    public Integer getMaxBlockSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_MAX_BLOCK_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.valueOf(configProperties.get(key));
            }
        }
        return 64 * MB;
    }

    public Integer getMinBlockSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_MIN_BLOCK_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.valueOf(configProperties.get(key));
            }
        }
        return 5 * MB;
    }

    public Integer getReadBufferSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_READ_BUFFER_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.valueOf(configProperties.get(key));
            }
        }
        return MB;
    }

    public Integer getWriteBufferSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_WRITE_BUFFER_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.valueOf(configProperties.get(key));
            }
        }
        return 10 * MB;
    }

    public Supplier<Credentials> getProviderCredentials() {
        if (credentials == null) {
            getProvider().buildCredentials(this);
        }
        return credentials;
    }

    public void setProviderCredentials(Supplier<Credentials> credentials) {
        this.credentials = credentials;
    }

    public void validate() {
        getProvider().validate(this);
    }

    public ProviderMetadata getProviderMetadata() {
        return getProvider().getProviderMetadata();
    }

    public BlobStoreLocation getBlobStoreLocation() {
        return new BlobStoreLocation(getOffloadDriverMetadata());
    }

    public BlobStore getBlobStore() {
        return getProvider().getBlobStore(this);
    }

    public Map<String, String> getOffloadDriverMetadata() {
        return ImmutableMap.of(
                BLOB_STORE_PROVIDER_KEY, (getProvider() != null) ? getProvider().toString() : "",
                METADATA_FIELD_BUCKET,  (getBucket() != null) ?  getBucket() : "",
                METADATA_FIELD_REGION, (getRegion() != null) ? getRegion() : "",
                METADATA_FIELD_ENDPOINT, (getServiceEndpoint() != null) ? getServiceEndpoint() : ""
             );
    }

    protected Properties getOverrides() {
        Properties overrides = new Properties();
        // This property controls the number of parts being uploaded in parallel.
        overrides.setProperty("jclouds.mpu.parallel.degree", "1");
        overrides.setProperty("jclouds.mpu.parts.size", Integer.toString(getMaxBlockSizeInBytes()));
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        if (getDriver().equalsIgnoreCase(AWS_S3.getDriver())) {
            ApiRegistry.registerApi(new S3ApiMetadata());
            ProviderRegistry.registerProvider(new AWSS3ProviderMetadata());
        } else if (getDriver().equalsIgnoreCase(GOOGLE_CLOUD_STORAGE.getDriver())) {
            ProviderRegistry.registerProvider(new GoogleCloudStorageProviderMetadata());
        }

        if (StringUtils.isNotEmpty(getServiceEndpoint())) {
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }

        // load more jclouds properties into the overrides
        System.getProperties().entrySet().stream()
            .filter(p -> p.getKey().toString().startsWith("jclouds"))
            .forEach(jcloudsProp -> {
                overrides.setProperty(jcloudsProp.getKey().toString(), jcloudsProp.getValue().toString());
            });

        System.getenv().entrySet().stream()
            .filter(p -> p.getKey().toString().startsWith("jclouds"))
            .forEach(jcloudsProp -> {
                overrides.setProperty(jcloudsProp.getKey().toString(), jcloudsProp.getValue().toString());
            });

        log.info("getOverrides: {}", overrides.toString());
        return overrides;
    }

    /*
     * Interfaces for the JCloudBlobStoreProvider's to implement
     */
    /**
     * Checks the given TieredStorageConfiguration to see if all of the
     * required properties are set, and that all properties are valid.
     */
    public interface ConfigValidation {
        void validate(TieredStorageConfiguration config) throws IllegalArgumentException;
    }

    /**
     * Constructs the proper credentials for the given JCloud provider
     * from the given TieredStorageConfiguration.
     */
    public interface CredentialBuilder {
        void buildCredentials(TieredStorageConfiguration config);
    }

    /**
     * Builds a JCloudprovider BlobStore from the TieredStorageConfiguration,
     * which allows us to publish and retrieve data blocks from the external
     * storage system.
     */
    public interface BlobStoreBuilder {
        BlobStore getBlobStore(TieredStorageConfiguration config);
    }
}
