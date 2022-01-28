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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.common.nar.NarClassLoader;

/**
 * A cache entry in the function cache. Tracks which workers still reference
 * the dependencies. Once none reference it any more, the class loaders will
 * be cleaned up.
 */
@Slf4j
public class FunctionCacheEntry implements AutoCloseable {

    public static final String JAVA_INSTANCE_JAR_PROPERTY = "pulsar.functions.java.instance.jar";

    @Getter
    private final URLClassLoader classLoader;

    private final Set<String> executionHolders;

    @Getter
    private final Set<String> jarFiles;

    private final Set<String> classpaths;

    FunctionCacheEntry(Collection<String> requiredJarFiles,
                       Collection<URL> requiredClasspaths,
                       URL[] libraryURLs,
                       String initialInstanceId, ClassLoader rootClassLoader) {
        this.classLoader = FunctionClassLoaders.create(libraryURLs, rootClassLoader);

        this.classpaths = requiredClasspaths.stream()
            .map(URL::toString)
            .collect(Collectors.toSet());
        this.jarFiles = new HashSet<>(requiredJarFiles);
        this.executionHolders = new HashSet<>(Collections.singleton(initialInstanceId));
    }

    FunctionCacheEntry(String narArchive, String initialInstanceId, ClassLoader rootClassLoader,
                       String narExtractionDirectory) throws IOException {
        this.classLoader = NarClassLoader.getFromArchive(new File(narArchive), Collections.emptySet(),
                rootClassLoader, narExtractionDirectory);
        this.classpaths = Collections.emptySet();
        this.jarFiles = Collections.singleton(narArchive);
        this.executionHolders = new HashSet<>(Collections.singleton(initialInstanceId));
    }

    boolean isInstanceRegistered(String iid) {
        return executionHolders.contains(iid);
    }

    public void register(String eid,
                         Collection<String> requiredJarFiles,
                         Collection<URL> requiredClassPaths) {
        if (jarFiles.size() != requiredJarFiles.size()
            || !new HashSet<>(requiredJarFiles).containsAll(jarFiles)) {
            throw new IllegalStateException(
                "The function registration references a different set of jar files than "
                + " previous registrations for this function : old = " + jarFiles
                + ", new = " + requiredJarFiles);
        }

        if (classpaths.size() != requiredClassPaths.size()
            || !requiredClassPaths.stream().map(URL::toString).collect(Collectors.toSet())
                .containsAll(classpaths)) {
            throw new IllegalStateException(
                "The function registration references a different set of classpaths than "
                + " previous registrations for this function : old = " + classpaths
                + ", new = " + requiredClassPaths);
        }

        this.executionHolders.add(eid);
    }

    public boolean unregister(String eid) {
        this.executionHolders.remove(eid);
        return this.executionHolders.isEmpty();
    }

    @Override
    public void close() {
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to release function code class loader for "
                + Arrays.toString(jarFiles.toArray()));
        }
    }
}
