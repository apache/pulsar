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

package org.apache.pulsar.functions.utils.functioncache;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.functions.utils.Exceptions;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of {@link FunctionCacheManager}.
 */
public class FunctionCacheManagerImpl implements FunctionCacheManager {

    /** Registered Functions **/
    private final Map<String, FunctionCacheEntry> cacheFunctions;

    private ClassLoader rootClassLoader;

    public FunctionCacheManagerImpl(ClassLoader rootClassLoader) {
        this.cacheFunctions = new ConcurrentHashMap<>();
        this.rootClassLoader = rootClassLoader;
    }

    Map<String, FunctionCacheEntry> getCacheFunctions() {
        return cacheFunctions;
    }

    @Override
    public ClassLoader getClassLoader(String fid) {
        if (fid == null) {
            throw new IllegalArgumentException("FunctionID not set");
        }

        synchronized (cacheFunctions) {
            FunctionCacheEntry entry = cacheFunctions.get(fid);
            if (entry == null) {
                throw new IllegalStateException("No dependencies are registered for function " + fid);
            }
            return entry.getClassLoader();
        }
    }

    @Override
    public void registerFunctionInstance(String fid,
                                         String eid,
                                         List<String> requiredJarFiles,
                                         List<URL> requiredClasspaths)
            throws IOException {
        if (fid == null) {
            throw new NullPointerException("FunctionID not set");
        }

        synchronized (cacheFunctions) {
            final AtomicBoolean computed = new AtomicBoolean(false);
            final AtomicReference<Throwable> throwable = new AtomicReference<>();
            FunctionCacheEntry entry = cacheFunctions.computeIfAbsent(fid, __ -> {
                URL[] urls = new URL[requiredJarFiles.size() + requiredClasspaths.size()];
                int count = 0;
                try {
                    // add jar files to urls
                    for (String jarFile : requiredJarFiles) {
                        urls[count++] = new File(jarFile).toURI().toURL();
                    }

                    // add classpaths
                    for (URL url : requiredClasspaths) {
                        urls[count++] = url;
                    }
                    final FunctionCacheEntry cacheEntry = new FunctionCacheEntry(
                            requiredJarFiles, requiredClasspaths, urls, eid, rootClassLoader);
                    computed.set(true);
                    return cacheEntry;
                } catch (Throwable cause) {
                    throwable.set(cause);
                    return null;
                }
            });
            if (throwable.get() != null) {
                Exceptions.rethrowIOException(throwable.get());
            }
            if (!computed.get() && entry != null) { // the key already exists
                entry.register(
                        eid,
                        requiredJarFiles,
                        requiredClasspaths);
            }
        }
    }

    @Override
    public void registerFunctionInstanceWithArchive(String fid, String eid,
                                                    String narArchive,
                                                    String narExtractionDirectory) throws IOException {
        if (fid == null) {
            throw new NullPointerException("FunctionID not set");
        }

        synchronized (cacheFunctions) {
            // Create new cache entry.
            final AtomicBoolean computed = new AtomicBoolean(false);
            final AtomicReference<Throwable> throwable = new AtomicReference<>();
            FunctionCacheEntry entry = cacheFunctions.computeIfAbsent(fid, __ -> {
                try {
                    final FunctionCacheEntry cacheEntry =
                            new FunctionCacheEntry(narArchive, eid, rootClassLoader, narExtractionDirectory);
                    computed.set(true);
                    return cacheEntry;
                } catch (Throwable cause) {
                    throwable.set(cause);
                    return null;
                }
            });
            if (throwable.get() != null) {
                Exceptions.rethrowIOException(throwable.get());
            }
            if (null != entry && computed.get()) {
                entry.register(eid, Collections.singleton(narArchive), Collections.emptyList());
            }
        }
    }

    @Override
    public void unregisterFunctionInstance(String fid,
                                           String eid) {
        synchronized (cacheFunctions) {
            FunctionCacheEntry entry = cacheFunctions.get(fid);

            if (null != entry) {
                if (entry.unregister(eid)) {
                    cacheFunctions.remove(fid);
                    entry.close();
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (cacheFunctions) {
            cacheFunctions.values().forEach(FunctionCacheEntry::close);
        }
    }


}
