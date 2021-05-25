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

import static org.apache.pulsar.common.policies.data.OffloadPolicies.CONFIGURATION_FIELDS;
import static org.apache.pulsar.common.policies.data.OffloadPolicies.DELETION_LAG_NAME_IN_CONF_FILE;
import static org.apache.pulsar.common.policies.data.OffloadPolicies.DRIVER_NAMES;
import static org.apache.pulsar.common.policies.data.OffloadPolicies.OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FieldParser;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;

/**
 * Utils of Pulsar offload policies.
 */
@Slf4j
public class OffloadPoliciesUtil {
    public static OffloadPolicies create(String driver, String region, String bucket, String endpoint,
                                         String role, String roleSessionName,
                                         String credentialId, String credentialSecret,
                                         Integer maxBlockSizeInBytes, Integer readBufferSizeInBytes,
                                         Long offloadThresholdInBytes, Long offloadDeletionLagInMillis,
                                         OffloadPolicies.OffloadedReadPriority readPriority) {
        OffloadPolicies offloadPolicies = new OffloadPolicies();
        offloadPolicies.setManagedLedgerOffloadDriver(driver);
        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(offloadThresholdInBytes);
        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(offloadDeletionLagInMillis);

        offloadPolicies.setManagedLedgerOffloadBucket(bucket);
        offloadPolicies.setManagedLedgerOffloadRegion(region);
        offloadPolicies.setManagedLedgerOffloadServiceEndpoint(endpoint);
        offloadPolicies.setManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes);
        offloadPolicies.setManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        offloadPolicies.setManagedLedgerOffloadedReadPriority(readPriority);

        if (driver.equalsIgnoreCase(DRIVER_NAMES.get(0)) || driver.equalsIgnoreCase(DRIVER_NAMES.get(1))) {
            if (role != null) {
                offloadPolicies.setS3ManagedLedgerOffloadRole(role);
            }
            if (roleSessionName != null) {
                offloadPolicies.setS3ManagedLedgerOffloadRoleSessionName(roleSessionName);
            }
            if (credentialId != null) {
                offloadPolicies.setS3ManagedLedgerOffloadCredentialId(credentialId);
            }
            if (credentialSecret != null) {
                offloadPolicies.setS3ManagedLedgerOffloadCredentialSecret(credentialSecret);
            }
            offloadPolicies.setS3ManagedLedgerOffloadRegion(region);
            offloadPolicies.setS3ManagedLedgerOffloadBucket(bucket);
            offloadPolicies.setS3ManagedLedgerOffloadServiceEndpoint(endpoint);
            offloadPolicies.setS3ManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes);
            offloadPolicies.setS3ManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        } else if (driver.equalsIgnoreCase(DRIVER_NAMES.get(2))) {
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
                    f.set(data, FieldParser.value((String) properties.get(f.getName()), f));
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("failed to initialize %s field while setting value %s",
                                    f.getName(), properties.get(f.getName())), e);
                }
            }
        });
        data.compatibleWithBrokerConfigFile(properties);
        return data;
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
    public static OffloadPolicies mergeConfiguration(OffloadPolicies topicLevelPolicies,
                                                     OffloadPolicies nsLevelPolicies,
                                                     Properties brokerProperties) {
        try {
            boolean allConfigValuesAreNull = true;
            OffloadPolicies offloadPolicies = new OffloadPolicies();
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
     * <p>The names of the fields {@link OffloadPolicies#managedLedgerOffloadDeletionLagInMillis}
     * and {@link OffloadPolicies#managedLedgerOffloadThresholdInBytes} are not matched with
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
        return FieldParser.value((String) object, field);
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
    public static OffloadPolicies oldPoliciesCompatible(OffloadPolicies nsLevelPolicies, Policies policies) {
        if (policies == null || (policies.offload_threshold == -1 && policies.offload_deletion_lag_ms == null)) {
            return nsLevelPolicies;
        }
        if (nsLevelPolicies == null) {
            nsLevelPolicies = new OffloadPolicies();
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

}
