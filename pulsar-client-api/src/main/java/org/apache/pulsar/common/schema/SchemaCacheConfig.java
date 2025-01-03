/*
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
package org.apache.pulsar.common.schema;

import java.util.Map;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Configuration for schema cache.
 */
@Data
@Accessors(chain = true)
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SchemaCacheConfig {

    /**
     * Maximum number of schema instances to cache.
     */
    public static final String SCHEMA_CACHE_SIZE = "schemaCache.maxSize";
    
    /**
     * Whether schema caching is enabled.
     */
    public static final String SCHEMA_CACHE_ENABLED = "schemaCache.enabled";
    
    /**
     * Time in seconds after which cached schema instances expire.
     */
    public static final String SCHEMA_CACHE_EXPIRE_SECONDS = "schemaCache.expireSeconds";

    private static final int DEFAULT_MAX_SIZE = 1000;
    private static final boolean DEFAULT_ENABLED = true;
    private static final long DEFAULT_EXPIRE_SECONDS = 3600;

    @Min(1)
    @NotNull
    @FieldContext(
        doc = "Maximum number of schema instances to cache",
        required = false
    )
    private int maxSize = DEFAULT_MAX_SIZE;

    @NotNull
    @FieldContext(
        doc = "Whether schema caching is enabled",
        required = false
    )
    private boolean enabled = DEFAULT_ENABLED;

    @Min(1)
    @NotNull
    @FieldContext(
        doc = "Time in seconds after which cached schema instances expire",
        required = false
    )
    private long expireSeconds = DEFAULT_EXPIRE_SECONDS;

    public SchemaCacheConfig() {
        // For deserialization
    }

    private SchemaCacheConfig(int maxSize, boolean enabled, long expireSeconds) {
        this.maxSize = maxSize;
        this.enabled = enabled;
        this.expireSeconds = expireSeconds;
    }

    /**
     * Create a new config instance from properties.
     *
     * @param properties configuration properties
     * @return new config instance
     */
    public static SchemaCacheConfig create(Map<String, Object> properties) {
        int maxSize = getIntValue(properties, SCHEMA_CACHE_SIZE, DEFAULT_MAX_SIZE);
        boolean enabled = getBooleanValue(properties, SCHEMA_CACHE_ENABLED, DEFAULT_ENABLED);
        long expireSeconds = getLongValue(properties, SCHEMA_CACHE_EXPIRE_SECONDS, DEFAULT_EXPIRE_SECONDS);
        
        return new SchemaCacheConfig(maxSize, enabled, expireSeconds);
    }

    private static int getIntValue(Map<String, Object> properties, String key, int defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private static boolean getBooleanValue(Map<String, Object> properties, String key, boolean defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private static long getLongValue(Map<String, Object> properties, String key, long defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    /**
     * Validates the configuration values.
     *
     * @throws IllegalArgumentException if any configuration value is invalid
     */
    public void validate() {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be greater than 0");
        }
        if (expireSeconds < 1) {
            throw new IllegalArgumentException("expireSeconds must be greater than 0");
        }
    }

    @Override
    public String toString() {
        return "SchemaCacheConfig("
            + "maxSize=" + maxSize
            + ", enabled=" + enabled
            + ", expireSeconds=" + expireSeconds
            + ")";
    }
}