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

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Definition of the offload policies.
 */
@Slf4j
@Data
@ToString
public class OffloadPolicies implements Serializable {

    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public enum OffloadedReadPriority {
        /**
         * For offloaded messages, readers will try to read from bookkeeper at first,
         * if messages not exist at bookkeeper then read from offloaded storage.
         */
        BOOKKEEPER_FIRST("bookkeeper-first"),
        /**
         * For offloaded messages, readers will try to read from offloaded storage first,
         * even they are still exist in bookkeeper.
         */
        TIERED_STORAGE_FIRST("tiered-storage-first");

        private final String value;

        OffloadedReadPriority(String value) {
            this.value = value;
        }

        public boolean equalsName(String otherName) {
            return value.equals(otherName);
        }

        @Override
        public String toString() {
            return value;
        }

        public static OffloadedReadPriority fromString(String str) {
            for (OffloadedReadPriority value : OffloadedReadPriority.values()) {
                if (value.value.equals(str)) {
                    return value;
                }
            }

            throw new IllegalArgumentException("--offloadedReadPriority parameter must be one of "
                    + Arrays.stream(OffloadedReadPriority.values())
                    .map(OffloadedReadPriority::toString)
                    .collect(Collectors.joining(","))
                    + " but got: " + str);
        }

        public String getValue() {
            return value;
        }
    }

    private final static long serialVersionUID = 0L;

    final static List<Field> CONFIGURATION_FIELDS;

    static {
        CONFIGURATION_FIELDS = new ArrayList<>();
        Class<OffloadPolicies> clazz = OffloadPolicies.class;
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Configuration.class)) {
                CONFIGURATION_FIELDS.add(field);
            }
        }
    }

    public final static int DEFAULT_MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024 * 1024;   // 64MB
    public final static int DEFAULT_READ_BUFFER_SIZE_IN_BYTES = 1024 * 1024;      // 1MB
    public final static int DEFAULT_OFFLOAD_MAX_THREADS = 2;
    public final static int DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS = 1;
    public final static List<String> DRIVER_NAMES = Collections.unmodifiableList(
            Arrays.asList("S3", "aws-s3", "google-cloud-storage", "filesystem", "azureblob", "aliyun-oss")
    );
    public final static String DEFAULT_OFFLOADER_DIRECTORY = "./offloaders";
    public final static Long DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES = null;
    public final static Long DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS = null;

    public final static String OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE =
            "managedLedgerOffloadAutoTriggerSizeThresholdBytes";
    public final static String DELETION_LAG_NAME_IN_CONF_FILE = "managedLedgerOffloadDeletionLagMs";
    public final static OffloadedReadPriority DEFAULT_OFFLOADED_READ_PRIORITY =
            OffloadedReadPriority.TIERED_STORAGE_FIRST;

    // common config
    @Configuration
    String offloadersDirectory = DEFAULT_OFFLOADER_DIRECTORY;

    @Configuration
    String managedLedgerOffloadDriver = null;

    @Configuration
    Integer managedLedgerOffloadMaxThreads = DEFAULT_OFFLOAD_MAX_THREADS;

    @Configuration
    Integer managedLedgerOffloadPrefetchRounds = DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS;

    @Configuration
    Long managedLedgerOffloadThresholdInBytes = DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;

    @Configuration
    Long managedLedgerOffloadDeletionLagInMillis = DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;

    @Configuration
    OffloadedReadPriority managedLedgerOffloadedReadPriority = DEFAULT_OFFLOADED_READ_PRIORITY;

    // s3 config, set by service configuration or cli
    @Configuration
    String s3ManagedLedgerOffloadRegion = null;

    @Configuration
    String s3ManagedLedgerOffloadBucket = null;

    @Configuration
    String s3ManagedLedgerOffloadServiceEndpoint = null;

    @Configuration
    Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;

    @Configuration
    Integer s3ManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;

    // s3 config, set by service configuration
    @Configuration
    String s3ManagedLedgerOffloadCredentialId = null;

    @Configuration
    String s3ManagedLedgerOffloadCredentialSecret = null;

    @Configuration
    String s3ManagedLedgerOffloadRole = null;

    @Configuration
    String s3ManagedLedgerOffloadRoleSessionName = "pulsar-s3-offload";

    // gcs config, set by service configuration or cli
    @Configuration
    String gcsManagedLedgerOffloadRegion = null;

    @Configuration
    String gcsManagedLedgerOffloadBucket = null;

    @Configuration
    Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;

    @Configuration
    Integer gcsManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;

    // gcs config, set by service configuration
    @Configuration
    String gcsManagedLedgerOffloadServiceAccountKeyFile = null;

    // file system config, set by service configuration
    @Configuration
    String fileSystemProfilePath = null;

    @Configuration
    String fileSystemURI = null;

    // --------- new offload configurations ---------
    // they are universal configurations and could be used to `aws-s3`, `google-cloud-storage` or `azureblob`.
    @Configuration
    String managedLedgerOffloadBucket;

    @Configuration
    String managedLedgerOffloadRegion;

    @Configuration
    String managedLedgerOffloadServiceEndpoint;

    @Configuration
    Integer managedLedgerOffloadMaxBlockSizeInBytes;

    @Configuration
    Integer managedLedgerOffloadReadBufferSizeInBytes;

    void compatibleWithBrokerConfigFile(Properties properties) {
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
    }

    public boolean driverSupported() {
        return DRIVER_NAMES.stream().anyMatch(d -> d.equalsIgnoreCase(this.managedLedgerOffloadDriver));
    }

    public static String getSupportedDriverNames() {
        return String.join(",", DRIVER_NAMES);
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
        if (managedLedgerOffloadBucket != null && !managedLedgerOffloadBucket.isEmpty()) {
            return true;
        }
        if (isS3Driver()) {
            return s3ManagedLedgerOffloadBucket != null && !s3ManagedLedgerOffloadBucket.isEmpty();
        } else if (isGcsDriver()) {
            return gcsManagedLedgerOffloadBucket != null && !gcsManagedLedgerOffloadBucket.isEmpty();
        } else if (isFileSystemDriver()) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                managedLedgerOffloadedReadPriority,
                managedLedgerOffloadDriver,
                managedLedgerOffloadMaxThreads,
                managedLedgerOffloadPrefetchRounds,
                managedLedgerOffloadThresholdInBytes,
                managedLedgerOffloadDeletionLagInMillis,
                s3ManagedLedgerOffloadRegion,
                s3ManagedLedgerOffloadBucket,
                s3ManagedLedgerOffloadServiceEndpoint,
                s3ManagedLedgerOffloadMaxBlockSizeInBytes,
                s3ManagedLedgerOffloadReadBufferSizeInBytes,
                s3ManagedLedgerOffloadRole,
                s3ManagedLedgerOffloadRoleSessionName,
                gcsManagedLedgerOffloadRegion,
                gcsManagedLedgerOffloadBucket,
                gcsManagedLedgerOffloadMaxBlockSizeInBytes,
                gcsManagedLedgerOffloadReadBufferSizeInBytes,
                gcsManagedLedgerOffloadServiceAccountKeyFile,
                fileSystemProfilePath,
                fileSystemURI,
                managedLedgerOffloadBucket,
                managedLedgerOffloadRegion,
                managedLedgerOffloadServiceEndpoint,
                managedLedgerOffloadMaxBlockSizeInBytes,
                managedLedgerOffloadReadBufferSizeInBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OffloadPolicies other = (OffloadPolicies) obj;
        return Objects.equals(managedLedgerOffloadedReadPriority, other.getManagedLedgerOffloadedReadPriority())
                && Objects.equals(managedLedgerOffloadDriver, other.getManagedLedgerOffloadDriver())
                && Objects.equals(managedLedgerOffloadMaxThreads, other.getManagedLedgerOffloadMaxThreads())
                && Objects.equals(managedLedgerOffloadPrefetchRounds, other.getManagedLedgerOffloadPrefetchRounds())
                && Objects.equals(managedLedgerOffloadThresholdInBytes,
                other.getManagedLedgerOffloadThresholdInBytes())
                && Objects.equals(managedLedgerOffloadDeletionLagInMillis,
                other.getManagedLedgerOffloadDeletionLagInMillis())
                && Objects.equals(s3ManagedLedgerOffloadRegion, other.getS3ManagedLedgerOffloadRegion())
                && Objects.equals(s3ManagedLedgerOffloadBucket, other.getS3ManagedLedgerOffloadBucket())
                && Objects.equals(s3ManagedLedgerOffloadServiceEndpoint,
                other.getS3ManagedLedgerOffloadServiceEndpoint())
                && Objects.equals(s3ManagedLedgerOffloadMaxBlockSizeInBytes,
                    other.getS3ManagedLedgerOffloadMaxBlockSizeInBytes())
                && Objects.equals(s3ManagedLedgerOffloadReadBufferSizeInBytes,
                    other.getS3ManagedLedgerOffloadReadBufferSizeInBytes())
                && Objects.equals(s3ManagedLedgerOffloadRole, other.getS3ManagedLedgerOffloadRole())
                && Objects.equals(s3ManagedLedgerOffloadRoleSessionName,
                    other.getS3ManagedLedgerOffloadRoleSessionName())
                && Objects.equals(gcsManagedLedgerOffloadRegion, other.getGcsManagedLedgerOffloadRegion())
                && Objects.equals(gcsManagedLedgerOffloadBucket, other.getGcsManagedLedgerOffloadBucket())
                && Objects.equals(gcsManagedLedgerOffloadMaxBlockSizeInBytes,
                    other.getGcsManagedLedgerOffloadMaxBlockSizeInBytes())
                && Objects.equals(gcsManagedLedgerOffloadReadBufferSizeInBytes,
                    other.getGcsManagedLedgerOffloadReadBufferSizeInBytes())
                && Objects.equals(gcsManagedLedgerOffloadServiceAccountKeyFile,
                    other.getGcsManagedLedgerOffloadServiceAccountKeyFile())
                && Objects.equals(fileSystemProfilePath, other.getFileSystemProfilePath())
                && Objects.equals(fileSystemURI, other.getFileSystemURI())
                && Objects.equals(managedLedgerOffloadBucket, other.getManagedLedgerOffloadBucket())
                && Objects.equals(managedLedgerOffloadRegion, other.getManagedLedgerOffloadRegion())
                && Objects.equals(managedLedgerOffloadServiceEndpoint, other.getManagedLedgerOffloadServiceEndpoint())
                && Objects.equals(managedLedgerOffloadMaxBlockSizeInBytes,
                    other.getManagedLedgerOffloadMaxBlockSizeInBytes())
                && Objects.equals(managedLedgerOffloadReadBufferSizeInBytes,
                    other.getManagedLedgerOffloadReadBufferSizeInBytes());
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        setProperty(properties, "managedLedgerOffloadedReadPriority", this.getManagedLedgerOffloadedReadPriority());
        setProperty(properties, "offloadersDirectory", this.getOffloadersDirectory());
        setProperty(properties, "managedLedgerOffloadDriver", this.getManagedLedgerOffloadDriver());
        setProperty(properties, "managedLedgerOffloadMaxThreads",
                this.getManagedLedgerOffloadMaxThreads());
        setProperty(properties, "managedLedgerOffloadPrefetchRounds",
                this.getManagedLedgerOffloadPrefetchRounds());
        setProperty(properties, "managedLedgerOffloadThresholdInBytes",
                this.getManagedLedgerOffloadThresholdInBytes());
        setProperty(properties, "managedLedgerOffloadDeletionLagInMillis",
                this.getManagedLedgerOffloadDeletionLagInMillis());

        if (this.isS3Driver()) {
            setProperty(properties, "s3ManagedLedgerOffloadRegion",
                    this.getS3ManagedLedgerOffloadRegion());
            setProperty(properties, "s3ManagedLedgerOffloadBucket",
                    this.getS3ManagedLedgerOffloadBucket());
            setProperty(properties, "s3ManagedLedgerOffloadServiceEndpoint",
                    this.getS3ManagedLedgerOffloadServiceEndpoint());
            setProperty(properties, "s3ManagedLedgerOffloadMaxBlockSizeInBytes",
                    this.getS3ManagedLedgerOffloadMaxBlockSizeInBytes());
            setProperty(properties, "s3ManagedLedgerOffloadCredentialId",
                    this.getS3ManagedLedgerOffloadCredentialId());
            setProperty(properties, "s3ManagedLedgerOffloadCredentialSecret",
                    this.getS3ManagedLedgerOffloadCredentialSecret());
            setProperty(properties, "s3ManagedLedgerOffloadRole",
                    this.getS3ManagedLedgerOffloadRole());
            setProperty(properties, "s3ManagedLedgerOffloadRoleSessionName",
                    this.getS3ManagedLedgerOffloadRoleSessionName());
            setProperty(properties, "s3ManagedLedgerOffloadReadBufferSizeInBytes",
                    this.getS3ManagedLedgerOffloadReadBufferSizeInBytes());
        } else if (this.isGcsDriver()) {
            setProperty(properties, "gcsManagedLedgerOffloadRegion",
                    this.getGcsManagedLedgerOffloadRegion());
            setProperty(properties, "gcsManagedLedgerOffloadBucket",
                    this.getGcsManagedLedgerOffloadBucket());
            setProperty(properties, "gcsManagedLedgerOffloadMaxBlockSizeInBytes",
                    this.getGcsManagedLedgerOffloadMaxBlockSizeInBytes());
            setProperty(properties, "gcsManagedLedgerOffloadReadBufferSizeInBytes",
                    this.getGcsManagedLedgerOffloadReadBufferSizeInBytes());
            setProperty(properties, "gcsManagedLedgerOffloadServiceAccountKeyFile",
                    this.getGcsManagedLedgerOffloadServiceAccountKeyFile());
        } else if (this.isFileSystemDriver()) {
            setProperty(properties, "fileSystemProfilePath", this.getFileSystemProfilePath());
            setProperty(properties, "fileSystemURI", this.getFileSystemURI());
        }

        setProperty(properties, "managedLedgerOffloadBucket", this.getManagedLedgerOffloadBucket());
        setProperty(properties, "managedLedgerOffloadRegion", this.getManagedLedgerOffloadRegion());
        setProperty(properties, "managedLedgerOffloadServiceEndpoint",
                this.getManagedLedgerOffloadServiceEndpoint());
        setProperty(properties, "managedLedgerOffloadMaxBlockSizeInBytes",
                this.getManagedLedgerOffloadMaxBlockSizeInBytes());
        setProperty(properties, "managedLedgerOffloadReadBufferSizeInBytes",
                this.getManagedLedgerOffloadReadBufferSizeInBytes());

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

}
