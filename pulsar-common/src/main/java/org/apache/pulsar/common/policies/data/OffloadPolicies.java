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

import com.google.common.base.MoreObjects;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * Definition of the offload policies.
 */
@Data
public class OffloadPolicies {

    public final static int DEFAULT_MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024 * 1024;   // 64MB
    public final static int DEFAULT_READ_BUFFER_SIZE_IN_BYTES = 1024 * 1024;      // 1MB
    public final static int DEFAULT_OFFLOAD_MAX_THREADS = 2;
    public final static String[] DRIVER_NAMES = {"S3", "aws-s3", "google-cloud-storage", "filesystem"};
    public final static String DEFAULT_OFFLOADER_DIRECTORY = "./offloaders";
    public final static long DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES = -1;
    public final static Long DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS = null;

    // common config
    private String offloadersDirectory = DEFAULT_OFFLOADER_DIRECTORY;
    private String managedLedgerOffloadDriver = null;
    private int managedLedgerOffloadMaxThreads = DEFAULT_OFFLOAD_MAX_THREADS;
    private long managedLedgerOffloadThresholdInBytes = DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;
    private Long managedLedgerOffloadDeletionLagInMillis = DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;

    // s3 config, set by service configuration or cli
    private String s3ManagedLedgerOffloadRegion = null;
    private String s3ManagedLedgerOffloadBucket = null;
    private String s3ManagedLedgerOffloadServiceEndpoint = null;
    private int s3ManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    private int s3ManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // s3 config, set by service configuration
    private String s3ManagedLedgerOffloadRole = null;
    private String s3ManagedLedgerOffloadRoleSessionName = "pulsar-s3-offload";

    // gcs config, set by service configuration or cli
    private String gcsManagedLedgerOffloadRegion = null;
    private String gcsManagedLedgerOffloadBucket = null;
    private int gcsManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    private int gcsManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // gcs config, set by service configuration
    private String gcsManagedLedgerOffloadServiceAccountKeyFile = null;

    // file system config, set by service configuration
    private String fileSystemProfilePath = null;
    private String fileSystemURI = null;

    public static OffloadPolicies create(String driver, String region, String bucket, String endpoint,
                                         int maxBlockSizeInBytes, int readBufferSizeInBytes,
                                         long offloadThresholdInBytes, Long offloadDeletionLagInMillis) {
        OffloadPolicies offloadPolicies = new OffloadPolicies();
        offloadPolicies.setManagedLedgerOffloadDriver(driver);
        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(offloadThresholdInBytes);
        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(offloadDeletionLagInMillis);

        if (driver.equalsIgnoreCase(DRIVER_NAMES[0]) || driver.equalsIgnoreCase(DRIVER_NAMES[1])) {
            offloadPolicies.setS3ManagedLedgerOffloadRegion(region);
            offloadPolicies.setS3ManagedLedgerOffloadBucket(bucket);
            offloadPolicies.setS3ManagedLedgerOffloadServiceEndpoint(endpoint);
            offloadPolicies.setS3ManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes);
            offloadPolicies.setS3ManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        } else if (driver.equalsIgnoreCase(DRIVER_NAMES[2])) {
            offloadPolicies.setGcsManagedLedgerOffloadRegion(region);
            offloadPolicies.setGcsManagedLedgerOffloadBucket(bucket);
            offloadPolicies.setGcsManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes);
            offloadPolicies.setGcsManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        }
        return offloadPolicies;
    }

    public static OffloadPolicies create(Properties properties) {
        OffloadPolicies data = new OffloadPolicies();
        Field[] fields = OffloadPolicies.class.getDeclaredFields();
        Arrays.stream(fields).forEach(f -> {
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
        });
        return data;
    }

    public boolean driverSupported() {
        return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(this.managedLedgerOffloadDriver));
    }

    public static String getSupportedDriverNames() {
        return StringUtils.join(DRIVER_NAMES, ",");
    }

    public boolean isS3Driver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[0])
                || managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[1]);
    }

    public boolean isGcsDriver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[2]);
    }

    public boolean isFileSystemDriver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[3]);
    }

    public boolean bucketValid() {
        if (managedLedgerOffloadDriver == null) {
            return false;
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

    @Override
    public int hashCode() {
        return Objects.hash(
                managedLedgerOffloadDriver,
                managedLedgerOffloadMaxThreads,
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
                fileSystemURI);
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
        return Objects.equals(managedLedgerOffloadDriver, other.getManagedLedgerOffloadDriver())
                && Objects.equals(managedLedgerOffloadMaxThreads, other.getManagedLedgerOffloadMaxThreads())
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
                && Objects.equals(fileSystemURI, other.getFileSystemURI());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("managedLedgerOffloadDriver", managedLedgerOffloadDriver)
                .add("managedLedgerOffloadMaxThreads", managedLedgerOffloadMaxThreads)
                .add("managedLedgerOffloadThresholdInBytes", managedLedgerOffloadThresholdInBytes)
                .add("managedLedgerOffloadDeletionLagInMillis", managedLedgerOffloadDeletionLagInMillis)
                .add("s3ManagedLedgerOffloadRegion", s3ManagedLedgerOffloadRegion)
                .add("s3ManagedLedgerOffloadBucket", s3ManagedLedgerOffloadBucket)
                .add("s3ManagedLedgerOffloadServiceEndpoint", s3ManagedLedgerOffloadServiceEndpoint)
                .add("s3ManagedLedgerOffloadMaxBlockSizeInBytes", s3ManagedLedgerOffloadMaxBlockSizeInBytes)
                .add("s3ManagedLedgerOffloadReadBufferSizeInBytes", s3ManagedLedgerOffloadReadBufferSizeInBytes)
                .add("s3ManagedLedgerOffloadRole", s3ManagedLedgerOffloadRole)
                .add("s3ManagedLedgerOffloadRoleSessionName", s3ManagedLedgerOffloadRoleSessionName)
                .add("gcsManagedLedgerOffloadRegion", gcsManagedLedgerOffloadRegion)
                .add("gcsManagedLedgerOffloadBucket", gcsManagedLedgerOffloadBucket)
                .add("gcsManagedLedgerOffloadMaxBlockSizeInBytes", gcsManagedLedgerOffloadMaxBlockSizeInBytes)
                .add("gcsManagedLedgerOffloadReadBufferSizeInBytes", gcsManagedLedgerOffloadReadBufferSizeInBytes)
                .add("gcsManagedLedgerOffloadServiceAccountKeyFile", gcsManagedLedgerOffloadServiceAccountKeyFile)
                .add("fileSystemProfilePath", fileSystemProfilePath)
                .add("fileSystemURI", fileSystemURI)
                .toString();
    }

}
