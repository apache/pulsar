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

/**
 * Definition of the retention policy.
 *
 * <p>When you set a retention policy you must set **both** a *size limit* and a *time limit*.
 * In the case where you don't want to limit by either time or set, the value must be set to `-1`.
 * Retention policy will be effectively disabled and it won't prevent the deletion of acknowledged
 * messages when either size or time limit is set to `0`.
 * Infinite retention can be achieved by setting both time and size limits to `-1`.
 */
public class RetentionPolicies {

    public static final int DEFAULT_RETENTION_TIME_IN_MINUTES = 0;

    public static final long DEFAULT_RETENTION_SIZE_IN_MB = 0;

    private Integer retentionTimeInMinutes;
    private Long retentionSizeInMB;

    public RetentionPolicies() {
        this(null, null);
    }

    public RetentionPolicies(Integer retentionTimeInMinutes, Integer retentionSizeInMB) {
        this.retentionSizeInMB = retentionSizeInMB == null ? null : retentionSizeInMB.longValue();
        this.retentionTimeInMinutes = retentionTimeInMinutes;
    }

    public boolean allGatesNotSet(){
        return retentionTimeInMinutes == null && retentionSizeInMB == null;
    }

    public int getRetentionTimeInMinutes() {
        return retentionTimeInMinutes == null ? DEFAULT_RETENTION_TIME_IN_MINUTES : retentionTimeInMinutes;
    }

    public long getRetentionSizeInMB() {
        return retentionSizeInMB == null ? DEFAULT_RETENTION_SIZE_IN_MB : retentionSizeInMB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetentionPolicies that = (RetentionPolicies) o;

        if (getRetentionTimeInMinutes() != that.getRetentionTimeInMinutes()) {
            return false;
        }

        return getRetentionSizeInMB() == that.getRetentionSizeInMB();
    }

    @Override
    public int hashCode() {
        long result = getRetentionTimeInMinutes();
        result = 31 * result + getRetentionSizeInMB();
        return Long.hashCode(result);
    }

    @Override
    public String toString() {
        return "RetentionPolicies{" + "retentionTimeInMinutes=" + getRetentionTimeInMinutes() + ", retentionSizeInMB="
                + getRetentionSizeInMB() + '}';
    }
}
