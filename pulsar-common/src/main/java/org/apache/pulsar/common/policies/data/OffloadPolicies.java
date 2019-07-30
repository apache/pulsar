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

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.common.naming.NamespaceName;

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
public class OffloadPolicies {
    private String driver;
    private String endpoint;
    private String bucket;
    private long maxBlockSizeInBytes;
    private long readBufferSizeInBytes;

    public OffloadPolicies(String driver, String endpoint, String bucket, long maxBlockSizeInBytes, long readBufferSizeInBytes) {
        this.driver = driver;
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.maxBlockSizeInBytes = maxBlockSizeInBytes;
        this.readBufferSizeInBytes = readBufferSizeInBytes;
    }

    public String getDriver() {
        return driver;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getBucket() {
        return bucket;
    }

    public long getMaxBlockSizeInBytes() {
        return maxBlockSizeInBytes;
    }

    public long getReadBufferSizeInBytes() {
        return readBufferSizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffloadPolicies that = (OffloadPolicies) o;
        return maxBlockSizeInBytes == that.maxBlockSizeInBytes &&
                readBufferSizeInBytes == that.readBufferSizeInBytes &&
                Objects.equals(driver, that.driver) &&
                Objects.equals(endpoint, that.endpoint) &&
                Objects.equals(bucket, that.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(driver, endpoint, bucket, maxBlockSizeInBytes, readBufferSizeInBytes);
    }

    @Override
    public String toString() {
        return "OffloadPolicies{" +
                "driver=" + driver +
                ", endpoint='" + endpoint + '\'' +
                ", bucket='" + bucket + '\'' +
                ", maxBlockSizeInBytes=" + maxBlockSizeInBytes +
                ", readBufferSizeInBytes=" + readBufferSizeInBytes +
                '}';
    }
}
