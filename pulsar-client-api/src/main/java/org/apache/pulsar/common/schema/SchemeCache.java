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

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Cache for Schema instances to improve performance by reusing schema objects.
 */
@Slf4j
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SchemaCache {

    private final ConcurrentHashMap<SchemaType, WeakHashMap<Class<?>, Schema<?>>> typeSchemaCache;
    private final ReentrantReadWriteLock lock;
    private final SchemaCacheConfig config;
    private final SchemaCacheCleanupStrategy cleanupStrategy;

    public SchemaCache(SchemaCacheConfig config) {
        this.typeSchemaCache = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.config = config;
        this.cleanupStrategy = new SchemaCacheCleanupStrategy(config.getExpireSeconds());
        
        if (log.isInfoEnabled()) {
            log.info("Initializing schema cache with config: {}", config);
        }
    }

    /**
     * Get or create a schema instance from cache.
     *
     * @param clazz class to create schema for
     * @param type schema type
     * @param creator function to create new schema if not found in cache
     * @return schema instance (either from cache or newly created)
     */
    @SuppressWarnings("unchecked")
    public <T> Schema<T> getOrCreateSchema(Class<T> clazz, SchemaType type, 
            Function<Class<T>, Schema<T>> creator) {
        if (!config.isEnabled()) {
            if (log.isDebugEnabled()) {
                log.debug("Schema cache is disabled, creating new schema for class: {}", clazz.getName());
            }
            return creator.apply(clazz);
        }

        WeakHashMap<Class<?>, Schema<?>> schemaCache = typeSchemaCache
            .computeIfAbsent(type, k -> new WeakHashMap<>());

        // Try read lock first
        lock.readLock().lock();
        try {
            Schema<T> cachedSchema = (Schema<T>) schemaCache.get(clazz);
            if (cachedSchema != null) {
                cleanupStrategy.recordAccess(clazz);
                SchemaCacheMetrics.recordHit();
                if (log.isDebugEnabled()) {
                    log.debug("Schema cache hit for class: {}", clazz.getName());
                }
                return cloneSchema(cachedSchema);
            }
        } finally {
            lock.readLock().unlock();
        }

        // Use write lock to create new schema
        lock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            Schema<T> cachedSchema = (Schema<T>) schemaCache.get(clazz);
            if (cachedSchema != null) {
                cleanupStrategy.recordAccess(clazz);
                SchemaCacheMetrics.recordHit();
                if (log.isDebugEnabled()) {
                    log.debug("Schema cache hit after write lock for class: {}", clazz.getName());
                }
                return cloneSchema(cachedSchema);
            }

            // Check cache size and cleanup if needed
            if (schemaCache.size() >= config.getMaxSize()) {
                if (log.isDebugEnabled()) {
                    log.debug("Schema cache size ({}) reached limit, performing cleanup", schemaCache.size());
                }
                cleanupStrategy.cleanup(schemaCache);
            }

            // Create and cache new schema
            long startTime = System.currentTimeMillis();
            Schema<T> newSchema;
            try {
                newSchema = creator.apply(clazz);
            } catch (Exception e) {
                SchemaCacheMetrics.recordLoadError();
                log.error("Failed to create schema for class: " + clazz.getName(), e);
                throw new SchemaCacheException("Failed to create schema for class: " + clazz.getName(), e);
            }
            long loadTime = System.currentTimeMillis() - startTime;
            SchemaCacheMetrics.recordLoadTime(loadTime);

            schemaCache.put(clazz, newSchema);
            cleanupStrategy.recordAccess(clazz);
            SchemaCacheMetrics.recordMiss();
            SchemaCacheMetrics.updateSize(schemaCache.size());

            if (log.isDebugEnabled()) {
                log.debug("Created and cached new schema for class: {} (load time: {} ms)", 
                    clazz.getName(), loadTime);
            }

            return cloneSchema(newSchema);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Create a deep clone of the schema instance.
     *
     * @param original schema to clone
     * @return cloned schema instance
     */
    private static <T> Schema<T> cloneSchema(Schema<T> original) {
        SchemaInfo originalInfo = original.getSchemaInfo();
        SchemaInfo clonedInfo = SchemaInfo.builder()
            .name(originalInfo.getName())
            .schema(originalInfo.getSchema().clone())
            .type(originalInfo.getType())
            .properties(new HashMap<>(originalInfo.getProperties()))
            .timestamp(originalInfo.getTimestamp())
            .build();

        return DefaultImplementation
            .getDefaultImplementation()
            .getSchema(clonedInfo);
    }

    /**
     * Clear all entries from the cache.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            typeSchemaCache.clear();
            SchemaCacheMetrics.updateSize(0);
            if (log.isInfoEnabled()) {
                log.info("Schema cache cleared");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get current cache statistics.
     *
     * @return map of metric name to value
     */
    public Map<String, Number> getStats() {
        return SchemaCacheMetrics.getMetrics();
    }
}