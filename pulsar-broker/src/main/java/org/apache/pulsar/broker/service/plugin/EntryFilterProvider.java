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
package org.apache.pulsar.broker.service.plugin;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class EntryFilterProvider {

    static final String ENTRY_FILTER_DEFINITION_FILE = "entry_filter.yml";

    /**
     * create entry filter instance.
     */
    public static ImmutableMap<String, EntryFilterWithClassLoader> createEntryFilters(
            ServiceConfiguration conf) throws IOException {
        EntryFilterDefinitions definitions = searchForEntryFilters(conf.getEntryFiltersDirectory(),
                conf.getNarExtractionDirectory());
        ImmutableMap.Builder<String, EntryFilterWithClassLoader> builder = ImmutableMap.builder();
        for (String filterName : conf.getEntryFilterNames()) {
            EntryFilterMetaData metaData = definitions.getFilters().get(filterName);
            if (null == metaData) {
                throw new RuntimeException("No entry filter is found for name `" + filterName
                        + "`. Available entry filters are : " + definitions.getFilters());
            }
            EntryFilterWithClassLoader filter;
            filter = load(metaData, conf.getNarExtractionDirectory());
            if (filter != null) {
                builder.put(filterName, filter);
            }
            log.info("Successfully loaded entry filter for name `{}`", filterName);
        }
        return builder.build();
    }

    private static EntryFilterDefinitions searchForEntryFilters(String entryFiltersDirectory,
                                                                            String narExtractionDirectory)
            throws IOException {
        Path path = Paths.get(entryFiltersDirectory).toAbsolutePath();
        log.info("Searching for entry filters in {}", path);

        EntryFilterDefinitions entryFilterDefinitions = new EntryFilterDefinitions();
        if (!path.toFile().exists()) {
            log.info("Pulsar entry filters directory not found");
            return entryFilterDefinitions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    EntryFilterDefinition def =
                            getEntryFilterDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found entry filter from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getEntryFilterClass()));

                    EntryFilterMetaData metadata = new EntryFilterMetaData();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    entryFilterDefinitions.getFilters().put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load entry filters from {}."
                            + " It is OK however if you want to use this entry filters,"
                            + " please make sure you put the correct entry filter NAR"
                            + " package in the entry filter directory.", archive, t);
                }
            }
        }

        return entryFilterDefinitions;
    }

    private static EntryFilterDefinition getEntryFilterDefinition(String narPath,
                                                                              String narExtractionDirectory)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build()) {
            return getEntryFilterDefinition(ncl);
        }
    }

    private static EntryFilterDefinition getEntryFilterDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(
                configStr, EntryFilterDefinition.class
        );
    }

    private static EntryFilterWithClassLoader load(EntryFilterMetaData metadata,
                                                               String narExtractionDirectory)
            throws IOException {
        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(EntryFilter.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();
        EntryFilterDefinition def = getEntryFilterDefinition(ncl);
        if (StringUtils.isBlank(def.getEntryFilterClass())) {
            throw new IOException("Entry filters `" + def.getName() + "` does NOT provide a entry"
                    + " filters implementation");
        }

        try {
            Class entryFilterClass = ncl.loadClass(def.getEntryFilterClass());
            Object filter = entryFilterClass.getDeclaredConstructor().newInstance();
            if (!(filter instanceof EntryFilter)) {
                throw new IOException("Class " + def.getEntryFilterClass()
                        + " does not implement entry filter interface");
            }
            EntryFilter pi = (EntryFilter) filter;
            return new EntryFilterWithClassLoader(pi, ncl);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            log.error("Failed to load class {}", def.getEntryFilterClass(), e);
            throw new IOException(e);
        }
    }
}
