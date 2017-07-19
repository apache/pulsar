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
package org.apache.pulsar.common.io;

import org.apache.pulsar.common.io.fs.ResourceId;
import org.apache.pulsar.common.util.ReflectionHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// code based on apache beam io
public class FileSystems {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystems.class);

    private static final Pattern FILE_SCHEME_PATTERN =
            Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*):.*");

    private static final AtomicReference<Map<String, FileSystem>> SCHEME_TO_FILESYSTEM =
            new AtomicReference<>(Collections.unmodifiableMap(new HashMap<>()));


    public static WritableByteChannel create(ResourceId resourceId) throws IOException {
        return getFileSystemInternal(resourceId.getScheme()).create(resourceId);
    }

    public static ResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
        return getFileSystemInternal(parseScheme(singleResourceSpec))
                .matchNewResource(singleResourceSpec, isDirectory);
    }

    public static void register(Properties properties) {
        final Map<String, FileSystem> filesystems = new HashMap<>();
        // local
        FileSystem fs = new LocalFileSystemFactory().create(properties);
        filesystems.put(fs.getScheme(), fs);

        // load other file systems
        for (FileSystemFactory factory : getFactories()) {
            fs = factory.create(properties);
            if (filesystems.containsKey(fs.getScheme())) {
                throw new IllegalStateException(String.format(
                        "Scheme: [%s] has conflicting filesystems: [%s]",
                        fs.getScheme(),
                        fs.getClass().getName()));
            }
            filesystems.put(fs.getScheme(), fs);
        }

        SCHEME_TO_FILESYSTEM.set(Collections.unmodifiableMap(filesystems));
    }

    private static String parseScheme(String spec) {
        // The spec is almost, but not quite, a URI. In particular,
        // the reserved characters '[', ']', and '?' have meanings that differ
        // from their use in the URI spec. ('*' is not reserved).
        // Here, we just need the scheme, which is so circumscribed as to be
        // very easy to extract with a regex.
        final Matcher matcher = FILE_SCHEME_PATTERN.matcher(spec);

        if (!matcher.matches()) {
            return "file";
        } else {
            return matcher.group("scheme").toLowerCase();
        }
    }

    private static FileSystem getFileSystemInternal(String scheme) {
        String lowerCaseScheme = scheme.toLowerCase();
        final Map<String, FileSystem> schemeToFileSystem = SCHEME_TO_FILESYSTEM.get();
        FileSystem fileSystem = schemeToFileSystem.get(lowerCaseScheme);
        if (fileSystem != null) {
            return fileSystem;
        }

        throw new IllegalStateException("Unable to find registrar for " + scheme);
    }


    private static List<FileSystemFactory> getFactories() {
        final List<FileSystemFactory> factories = new ArrayList<>();
        Iterator<FileSystemFactory> iterator = ServiceLoader.load(FileSystemFactory.class,
                ReflectionHelper.findClassLoader()).iterator();

        while(iterator.hasNext()) {
            factories.add(iterator.next());
        }

        return factories;
    }

    private FileSystems() {}
}
