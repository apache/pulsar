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

import static org.apache.pulsar.common.util.FieldParser.value;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Definition of the offload policies.
 */
@Slf4j
@Data
@NoArgsConstructor
public class OffloadPoliciesImpl implements Serializable, OffloadPolicies {

    private static final long serialVersionUID = 0L;

    public static final List<Field> CONFIGURATION_FIELDS;

    static {
        List<Field> temp = new ArrayList<>();
        Class<OffloadPoliciesImpl> clazz = OffloadPoliciesImpl.class;
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Configuration.class)) {
                temp.add(field);
            }
        }
        CONFIGURATION_FIELDS = Collections.unmodifiableList(temp);
    }

    public static final int DEFAULT_MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024 * 1024;   // 64MB
    public static final int DEFAULT_READ_BUFFER_SIZE_IN_BYTES = 1024 * 1024;      // 1MB
    public static final int DEFAULT_OFFLOAD_MAX_THREADS = 2;
    public static final int DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS = 1;
    public static final ImmutableList<String> DRIVER_NAMES = ImmutableList
            .of("S3", "aws-s3", "google-cloud-storage", "filesystem", "azureblob", "aliyun-oss");
    public static final String DEFAULT_OFFLOADER_DIRECTORY = "./offloaders";
    public static final Long DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES = null;
    public static final Long DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS = null;

    public static final String OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE =
            "managedLedgerOffloadAutoTriggerSizeThresholdBytes";
    public static final String DELETION_LAG_NAME_IN_CONF_FILE = "managedLedgerOffloadDeletionLagMs";
    public static final OffloadedReadPriority DEFAULT_OFFLOADED_READ_PRIORITY =
            OffloadedReadPriority.TIERED_STORAGE_FIRST;

    // common config
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String offloadersDirectory = DEFAULT_OFFLOADER_DIRECTORY;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadDriver = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadMaxThreads = DEFAULT_OFFLOAD_MAX_THREADS;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadPrefetchRounds = DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Long managedLedgerOffloadThresholdInBytes = DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Long managedLedgerOffloadDeletionLagInMillis = DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private OffloadedReadPriority managedLedgerOffloadedReadPriority = DEFAULT_OFFLOADED_READ_PRIORITY;

    // s3 config, set by service configuration or cli
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadRegion = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadBucket = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadServiceEndpoint = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer s3ManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // s3 config, set by service configuration
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadCredentialId = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadCredentialSecret = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadRole = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadRoleSessionName = "pulsar-s3-offload";

    // gcs config, set by service configuration or cli
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String gcsManagedLedgerOffloadRegion = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String gcsManagedLedgerOffloadBucket = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer gcsManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // gcs config, set by service configuration
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String gcsManagedLedgerOffloadServiceAccountKeyFile = null;

    // file system config, set by service configuration
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String fileSystemProfilePath = null;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String fileSystemURI = null;

    // --------- new offload configurations ---------
    // they are universal configurations and could be used to `aws-s3`, `google-cloud-storage` or `azureblob`.
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadBucket;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadRegion;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadServiceEndpoint;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadMaxBlockSizeInBytes;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadReadBufferSizeInBytes;

    public static OffloadPoliciesImpl create(String driver, String region, String bucket, String endpoint,
                                             String role, String roleSessionName,
                                             String credentialId, String credentialSecret,
                                             Integer maxBlockSizeInBytes, Integer readBufferSizeInBytes,
                                             Long offloadThresholdInBytes, Long offloadDeletionLagInMillis,
                                             OffloadedReadPriority readPriority) {
        OffloadPoliciesImplBuilder builder = builder()
                .managedLedgerOffloadDriver(driver)
                .managedLedgerOffloadThresholdInBytes(offloadThresholdInBytes)
                .managedLedgerOffloadDeletionLagInMillis(offloadDeletionLagInMillis)
                .managedLedgerOffloadBucket(bucket)
                .managedLedgerOffloadRegion(region)
                .managedLedgerOffloadServiceEndpoint(endpoint)
                .managedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes)
                .managedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes)
                .managedLedgerOffloadedReadPriority(readPriority);

        if (driver.equalsIgnoreCase(DRIVER_NAMES.get(0)) || driver.equalsIgnoreCase(DRIVER_NAMES.get(1))) {
            if (role != null) {
                builder.s3ManagedLedgerOffloadRole(role);
            }
            if (roleSessionName != null) {
                builder.s3ManagedLedgerOffloadRoleSessionName(roleSessionName);
            }
            if (credentialId != null) {
                builder.s3ManagedLedgerOffloadCredentialId(credentialId);
            }
            if (credentialSecret != null) {
                builder.s3ManagedLedgerOffloadCredentialSecret(credentialSecret);
            }

            builder.s3ManagedLedgerOffloadRegion(region)
                    .s3ManagedLedgerOffloadBucket(bucket)
                    .s3ManagedLedgerOffloadServiceEndpoint(endpoint)
                    .s3ManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes)
                    .s3ManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        } else if (driver.equalsIgnoreCase(DRIVER_NAMES.get(2))) {
            builder.gcsManagedLedgerOffloadRegion(region)
                .gcsManagedLedgerOffloadBucket(bucket)
                .gcsManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes)
                .gcsManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        }

        return builder.build();
    }

    public static OffloadPoliciesImpl create(Properties properties) {
        OffloadPoliciesImpl data = new OffloadPoliciesImpl();
        for (Field f : CONFIGURATION_FIELDS) {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    f.set(data, value((String) properties.get(f.getName()), f));
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("failed to initialize %s field while setting value %s",
                                    f.getName(), properties.get(f.getName())), e);
                }
            }
        }
        data.compatibleWithBrokerConfigFile(properties);
        return data;
    }

    public static OffloadPoliciesImplBuilder builder() {
        return new OffloadPoliciesImplBuilder();
    }

    public void compatibleWithBrokerConfigFile(Properties properties) {
        if (!properties.containsKey("managedLedgerOffloadThresholdInBytes")
                && properties.containsKey(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE)) {
            setManagedLedgerOffloadThresholdInBytes(
                    Long.parseLong(properties.getProperty(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE)));
        }

        if (!properties.containsKey("managedLedgerOffloadDeletionLagInMillis")
                && properties.containsKey(DELETION_LAG_NAME_IN_CONF_FILE)) {
            setManagedLedgerOffloadDeletionLagInMillis(
                    Long.parseLong(properties.getProperty(DELETION_LAG_NAME_IN_CONF_FILE)));
        }

        if (properties.containsKey("managedLedgerDataReadPriority")) {
            setManagedLedgerOffloadedReadPriority(
                    OffloadedReadPriority.fromString(properties.getProperty("managedLedgerDataReadPriority")));
        }
    }

    public boolean driverSupported() {
        return DRIVER_NAMES.stream().anyMatch(d -> d.equalsIgnoreCase(this.managedLedgerOffloadDriver));
    }

    public static String getSupportedDriverNames() {
        return StringUtils.join(DRIVER_NAMES, ",");
    }

    public boolean isS3Driver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(0))
                || managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(1));
    }

    public boolean isGcsDriver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(2));
    }

    public boolean isFileSystemDriver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(3));
    }

    public boolean bucketValid() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        if (StringUtils.isNotEmpty(managedLedgerOffloadBucket)) {
            return true;
        }
        if (isS3Driver()) {
            return StringUtils.isNotEmpty(s3ManagedLedgerOffloadBucket);
        } else if (isGcsDriver()) {
            return StringUtils.isNotEmpty(gcsManagedLedgerOffloadBucket);
        } else if (isFileSystemDriver()) {
            return true;
        }
        return false;
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        for (Field f : CONFIGURATION_FIELDS) {
            try {
                f.setAccessible(true);
                setProperty(properties, f.getName(), f.get(this));
            } catch (Exception e) {
                throw new IllegalArgumentException("An error occurred while processing the field: " + f.getName(), e);
            }
        }
        return properties;
    }

    private static void setProperty(Properties properties, String key, Object value) {
        if (value != null) {
            properties.setProperty(key, "" + value);
        }
    }

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface Configuration {

    }

    /**
     * This method is used to make a compatible with old policies.
     *
     * <p>The filed {@link Policies#offload_threshold} is primitive, so it can't be known whether it had been set.
     * In the old logic, if the field value is -1, it could be thought that the field had not been set.
     *
     * @param nsLevelPolicies  namespace level offload policies
     * @param policies namespace policies
     * @return offload policies
     */
    public static OffloadPoliciesImpl oldPoliciesCompatible(OffloadPoliciesImpl nsLevelPolicies, Policies policies) {
        if (policies == null || (policies.offload_threshold == -1 && policies.offload_deletion_lag_ms == null)) {
            return nsLevelPolicies;
        }
        if (nsLevelPolicies == null) {
            nsLevelPolicies = new OffloadPoliciesImpl();
        }
        if (nsLevelPolicies.getManagedLedgerOffloadThresholdInBytes() == null
                && policies.offload_threshold != -1) {
            nsLevelPolicies.setManagedLedgerOffloadThresholdInBytes(policies.offload_threshold);
        }
        if (nsLevelPolicies.getManagedLedgerOffloadDeletionLagInMillis() == null
                && policies.offload_deletion_lag_ms != null) {
            nsLevelPolicies.setManagedLedgerOffloadDeletionLagInMillis(policies.offload_deletion_lag_ms);
        }
        return nsLevelPolicies;
    }

    /**
     * Merge different level offload policies.
     *
     * <p>policies level priority: topic > namespace > broker
     *
     * @param topicLevelPolicies topic level offload policies
     * @param nsLevelPolicies namespace level offload policies
     * @param brokerProperties broker level offload configuration
     * @return offload policies
     */
    public static OffloadPoliciesImpl mergeConfiguration(OffloadPoliciesImpl topicLevelPolicies,
                                                         OffloadPoliciesImpl nsLevelPolicies,
                                                         Properties brokerProperties) {
        try {
            boolean allConfigValuesAreNull = true;
            OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
            for (Field field : CONFIGURATION_FIELDS) {
                Object object;
                if (topicLevelPolicies != null && field.get(topicLevelPolicies) != null) {
                    object = field.get(topicLevelPolicies);
                } else if (nsLevelPolicies != null && field.get(nsLevelPolicies) != null) {
                    object = field.get(nsLevelPolicies);
                } else {
                    object = getCompatibleValue(brokerProperties, field);
                }
                if (object != null) {
                    field.set(offloadPolicies, object);
                    if (allConfigValuesAreNull) {
                        allConfigValuesAreNull = false;
                    }
                }
            }
            if (allConfigValuesAreNull) {
                return null;
            } else {
                return offloadPolicies;
            }
        } catch (Exception e) {
            log.error("Failed to merge configuration.", e);
            return null;
        }
    }

    /**
     * Make configurations of the OffloadPolicies compatible with the config file.
     *
     * <p>The names of the fields {@link OffloadPoliciesImpl#managedLedgerOffloadDeletionLagInMillis}
     * and {@link OffloadPoliciesImpl#managedLedgerOffloadThresholdInBytes} are not matched with
     * config file (broker.conf or standalone.conf).
     *
     * @param properties broker configuration properties
     * @param field filed
     * @return field value
     */
    private static Object getCompatibleValue(Properties properties, Field field) {
        Object object;
        if (field.getName().equals("managedLedgerOffloadThresholdInBytes")) {
            object = properties.getProperty("managedLedgerOffloadThresholdInBytes",
                    properties.getProperty(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE));
        } else if (field.getName().equals("managedLedgerOffloadDeletionLagInMillis")) {
            object = properties.getProperty("managedLedgerOffloadDeletionLagInMillis",
                    properties.getProperty(DELETION_LAG_NAME_IN_CONF_FILE));
        } else {
            object = properties.get(field.getName());
        }
        return value((String) object, field);
    }

    public static class OffloadPoliciesImplBuilder implements OffloadPolicies.Builder {
        private OffloadPoliciesImpl impl = new OffloadPoliciesImpl();

        public OffloadPoliciesImplBuilder offloadersDirectory(String offloadersDirectory) {
            impl.offloadersDirectory = offloadersDirectory;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadDriver(String managedLedgerOffloadDriver) {
            impl.managedLedgerOffloadDriver = managedLedgerOffloadDriver;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadMaxThreads(Integer managedLedgerOffloadMaxThreads) {
            impl.managedLedgerOffloadMaxThreads = managedLedgerOffloadMaxThreads;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadPrefetchRounds(
                Integer managedLedgerOffloadPrefetchRounds) {
            impl.managedLedgerOffloadPrefetchRounds = managedLedgerOffloadPrefetchRounds;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadThresholdInBytes(
                Long managedLedgerOffloadThresholdInBytes) {
            impl.managedLedgerOffloadThresholdInBytes = managedLedgerOffloadThresholdInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadDeletionLagInMillis(
                Long managedLedgerOffloadDeletionLagInMillis) {
            impl.managedLedgerOffloadDeletionLagInMillis = managedLedgerOffloadDeletionLagInMillis;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadedReadPriority(
                OffloadedReadPriority managedLedgerOffloadedReadPriority) {
            impl.managedLedgerOffloadedReadPriority = managedLedgerOffloadedReadPriority;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadRegion(String s3ManagedLedgerOffloadRegion) {
            impl.s3ManagedLedgerOffloadRegion = s3ManagedLedgerOffloadRegion;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadBucket(String s3ManagedLedgerOffloadBucket) {
            impl.s3ManagedLedgerOffloadBucket = s3ManagedLedgerOffloadBucket;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadServiceEndpoint(
                String s3ManagedLedgerOffloadServiceEndpoint) {
            impl.s3ManagedLedgerOffloadServiceEndpoint = s3ManagedLedgerOffloadServiceEndpoint;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadMaxBlockSizeInBytes(
                Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes) {
            impl.s3ManagedLedgerOffloadMaxBlockSizeInBytes = s3ManagedLedgerOffloadMaxBlockSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadReadBufferSizeInBytes(
                Integer s3ManagedLedgerOffloadReadBufferSizeInBytes) {
            impl.s3ManagedLedgerOffloadReadBufferSizeInBytes = s3ManagedLedgerOffloadReadBufferSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadCredentialId(
                String s3ManagedLedgerOffloadCredentialId) {
            impl.s3ManagedLedgerOffloadCredentialId = s3ManagedLedgerOffloadCredentialId;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadCredentialSecret(
                String s3ManagedLedgerOffloadCredentialSecret) {
            impl.s3ManagedLedgerOffloadCredentialSecret = s3ManagedLedgerOffloadCredentialSecret;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadRole(String s3ManagedLedgerOffloadRole) {
            impl.s3ManagedLedgerOffloadRole = s3ManagedLedgerOffloadRole;
            return this;
        }

        @Override
        public Builder setS3ManagedLedgerOffloadRoleSessionName(String s3ManagedLedgerOffloadRoleSessionName) {
            impl.s3ManagedLedgerOffloadRoleSessionName = s3ManagedLedgerOffloadRoleSessionName;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadRoleSessionName(
                String s3ManagedLedgerOffloadRoleSessionName) {
            impl.s3ManagedLedgerOffloadRoleSessionName = s3ManagedLedgerOffloadRoleSessionName;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadRegion(String gcsManagedLedgerOffloadRegion) {
            impl.gcsManagedLedgerOffloadRegion = gcsManagedLedgerOffloadRegion;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadBucket(String gcsManagedLedgerOffloadBucket) {
            impl.gcsManagedLedgerOffloadBucket = gcsManagedLedgerOffloadBucket;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadMaxBlockSizeInBytes(
                Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes) {
            impl.gcsManagedLedgerOffloadMaxBlockSizeInBytes = gcsManagedLedgerOffloadMaxBlockSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadReadBufferSizeInBytes(
                Integer gcsManagedLedgerOffloadReadBufferSizeInBytes) {
            impl.gcsManagedLedgerOffloadReadBufferSizeInBytes = gcsManagedLedgerOffloadReadBufferSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadServiceAccountKeyFile(
                String gcsManagedLedgerOffloadServiceAccountKeyFile) {
            impl.gcsManagedLedgerOffloadServiceAccountKeyFile = gcsManagedLedgerOffloadServiceAccountKeyFile;
            return this;
        }

        public OffloadPoliciesImplBuilder fileSystemProfilePath(String fileSystemProfilePath) {
            impl.fileSystemProfilePath = fileSystemProfilePath;
            return this;
        }

        public OffloadPoliciesImplBuilder fileSystemURI(String fileSystemURI) {
            impl.fileSystemURI = fileSystemURI;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadBucket(String managedLedgerOffloadBucket) {
            impl.managedLedgerOffloadBucket = managedLedgerOffloadBucket;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadRegion(String managedLedgerOffloadRegion) {
            impl.managedLedgerOffloadRegion = managedLedgerOffloadRegion;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadServiceEndpoint(
                String managedLedgerOffloadServiceEndpoint) {
            impl.managedLedgerOffloadServiceEndpoint = managedLedgerOffloadServiceEndpoint;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadMaxBlockSizeInBytes(
                Integer managedLedgerOffloadMaxBlockSizeInBytes) {
            impl.managedLedgerOffloadMaxBlockSizeInBytes = managedLedgerOffloadMaxBlockSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadReadBufferSizeInBytes(
                Integer managedLedgerOffloadReadBufferSizeInBytes) {
            impl.managedLedgerOffloadReadBufferSizeInBytes = managedLedgerOffloadReadBufferSizeInBytes;
            return this;
        }

        public OffloadPoliciesImpl build() {
            return impl;
        }
    }
}
