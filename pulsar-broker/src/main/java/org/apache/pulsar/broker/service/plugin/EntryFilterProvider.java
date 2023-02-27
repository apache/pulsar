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
package org.apache.pulsar.broker.service.plugin;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class EntryFilterProvider implements AutoCloseable {

    @VisibleForTesting
    static final String ENTRY_FILTER_DEFINITION_FILE = "entry_filter";

    private final ServiceConfiguration serviceConfiguration;
    @VisibleForTesting
    protected Map<String, EntryFilterMetaData> definitions;
    @VisibleForTesting
    protected Map<String, NarClassLoader> cachedClassLoaders;
    @VisibleForTesting
    protected List<EntryFilter> brokerEntryFilters;

    public EntryFilterProvider(ServiceConfiguration conf) throws IOException {
        this.serviceConfiguration = conf;
        initialize();
        initializeBrokerEntryFilters();
    }

    protected void initializeBrokerEntryFilters() throws IOException {
        if (!serviceConfiguration.getEntryFilterNames().isEmpty()) {
            brokerEntryFilters = loadEntryFilters(serviceConfiguration.getEntryFilterNames());
        } else {
            brokerEntryFilters = Collections.emptyList();
        }
    }

    public void validateEntryFilters(String entryFilterNames) throws InvalidEntryFilterException {
        if (StringUtils.isBlank(entryFilterNames)) {
            return;
        }
        final List<String> entryFilterList = readEntryFiltersString(entryFilterNames);
        for (String filterName : entryFilterList) {
            EntryFilterMetaData metaData = definitions.get(filterName);
            if (metaData == null) {
                throw new InvalidEntryFilterException("Entry filter '" + filterName + "' not found");
            }
        }
    }

    private List<String> readEntryFiltersString(String entryFilterNames) {
        final List<String> entryFilterList = Arrays.stream(entryFilterNames.split(","))
                .filter(n -> StringUtils.isNotBlank(n))
                .toList();
        return entryFilterList;
    }

    public List<EntryFilter> loadEntryFiltersForPolicy(EntryFilters policy)
            throws IOException {
        final String names = policy.getEntryFilterNames();
        if (StringUtils.isBlank(names)) {
            return Collections.emptyList();
        }
        final List<String> entryFilterList = readEntryFiltersString(names);
        return loadEntryFilters(entryFilterList);
    }

    private List<EntryFilter> loadEntryFilters(Collection<String> entryFilterNames)
            throws IOException {
        ImmutableMap.Builder<String, EntryFilter> builder = ImmutableMap.builder();
        for (String filterName : entryFilterNames) {
            EntryFilterMetaData metaData = definitions.get(filterName);
            if (null == metaData) {
                throw new RuntimeException("No entry filter is found for name `" + filterName
                        + "`. Available entry filters are : " + definitions.keySet());
            }
            final EntryFilter entryFilter = load(metaData);
            builder.put(filterName, entryFilter);
            log.info("Successfully loaded entry filter `{}`", filterName);
        }
        return builder.build().values().asList();
    }

    public List<EntryFilter> getBrokerEntryFilters() {
        return brokerEntryFilters;
    }

    private void initialize() throws IOException {
        final String entryFiltersDirectory = serviceConfiguration.getEntryFiltersDirectory();
        Path path = Paths.get(entryFiltersDirectory).toAbsolutePath();
        log.info("Searching for entry filters in {}", path);


        if (!path.toFile().exists()) {
            log.info("Pulsar entry filters directory not found");
            definitions = Collections.emptyMap();
            cachedClassLoaders = Collections.emptyMap();
            return;
        }
        Map<String, EntryFilterMetaData> entryFilterDefinitions = new HashMap<>();

        cachedClassLoaders = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    final NarClassLoader narClassLoader = loadNarClassLoader(archive);
                    EntryFilterDefinition def = getEntryFilterDefinition(narClassLoader);
                    log.info("Found entry filter from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getEntryFilterClass()));

                    EntryFilterMetaData metadata = new EntryFilterMetaData();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    entryFilterDefinitions.put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load entry filters from {}."
                            + " It is OK however if you want to use this entry filters,"
                            + " please make sure you put the correct entry filter NAR"
                            + " package in the entry filter directory.", archive, t);
                }
            }
        }
        definitions = Collections.unmodifiableMap(entryFilterDefinitions);
        cachedClassLoaders = Collections.unmodifiableMap(cachedClassLoaders);
    }

    @VisibleForTesting
    static EntryFilterDefinition getEntryFilterDefinition(NarClassLoader ncl) throws IOException {
        String configStr;

        try {
            configStr = ncl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE + ".yaml");
        } catch (NoSuchFileException e) {
            configStr = ncl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE + ".yml");
        }

        return ObjectMapperFactory.getYamlMapper().reader().readValue(
                configStr, EntryFilterDefinition.class
        );
    }

    protected EntryFilter load(EntryFilterMetaData metadata)
            throws IOException {
        final EntryFilterDefinition def = metadata.getDefinition();
        if (StringUtils.isBlank(def.getEntryFilterClass())) {
            throw new RuntimeException("Entry filter `" + def.getName() + "` does NOT provide a entry"
                    + " filters implementation");
        }
        try {
            final NarClassLoader ncl = getNarClassLoader(metadata.getArchivePath());
            if (ncl == null) {
                throw new RuntimeException("Entry filter `" + def.getName() + "` cannot be loaded, "
                        + "see the broker logs for further details");
            }
            Class entryFilterClass = ncl.loadClass(def.getEntryFilterClass());
            Object filter = entryFilterClass.getDeclaredConstructor().newInstance();
            if (!(filter instanceof EntryFilter)) {
                throw new IOException("Class " + def.getEntryFilterClass()
                        + " does not implement entry filter interface");
            }
            EntryFilter pi = (EntryFilter) filter;
            return new EntryFilterWithClassLoader(pi, ncl);
        } catch (Throwable e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            log.error("Failed to load class {}", metadata.getDefinition().getEntryFilterClass(), e);
            throw new IOException(e);
        }
    }

    private NarClassLoader getNarClassLoader(Path archivePath) {
        return cachedClassLoaders.get(classLoaderKey(archivePath));
    }

    private NarClassLoader loadNarClassLoader(Path archivePath) {
        final String absolutePath = classLoaderKey(archivePath);
        return cachedClassLoaders
                .computeIfAbsent(absolutePath, narFilePath -> {
                    try {
                        final File narFile = archivePath.toAbsolutePath().toFile();
                        return NarClassLoaderBuilder.builder()
                                .narFile(narFile)
                                .parentClassLoader(EntryFilter.class.getClassLoader())
                                .extractionDirectory(serviceConfiguration.getNarExtractionDirectory())
                                .build();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static String classLoaderKey(Path archivePath) {
        return archivePath.toString();
    }

    @Override
    public void close() throws Exception {
        brokerEntryFilters.forEach((filter) -> {
            try {
                filter.close();
            } catch (Throwable e) {
                log.warn("Error shutting down entry filter {}", filter, e);
            }
        });
        cachedClassLoaders.forEach((name, ncl) -> {
            try {
                ncl.close();
            } catch (Throwable e) {
                log.warn("Error closing entry filter class loader {}", name, e);
            }
        });
    }
}
