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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class EntryFilterProvider {

    static final String ENTRY_FILTER_DEFINITION_FILE = "entry_filter.yml";

    /**
     * create entry filter instance.
     */
    public static ImmutableMap<String, EntryFilter.EntryFilterWithClassLoader> createEntryFilters(
            ServiceConfiguration conf) throws IOException {
        EntryFilter.EntryFilterDefinitions definitions = searchForEntryFilters(conf.getEntryFiltersDirectory(),
                conf.getNarExtractionDirectory());
        ImmutableMap.Builder<String, EntryFilter.EntryFilterWithClassLoader> builder = ImmutableMap.builder();
        conf.getEntryFilterNames().forEach(filterName -> {
            EntryFilter.EntryFilterMetaData metaData = definitions.getFilters().get(filterName);
            if (null == metaData) {
                throw new RuntimeException("No entry filter is found for name `" + filterName
                        + "`. Available entry filters are : " + definitions.getFilters());
            }
            EntryFilter.EntryFilterWithClassLoader filter;
            try {
                filter = load(metaData, conf.getNarExtractionDirectory());
                if (filter != null) {
                    builder.put(filterName, filter);
                }
                log.info("Successfully loaded entry filter for name `{}`", filterName);
            } catch (IOException e) {
                log.error("Failed to load the entry filter for name `" + filterName + "`", e);
                throw new RuntimeException("Failed to load the broker interceptor for name `" + filterName + "`");
            }
        });
        return builder.build();
    }

    private static EntryFilter.EntryFilterDefinitions searchForEntryFilters(String entryFiltersDirectory,
                                                                            String narExtractionDirectory)
            throws IOException {
        Path path = Paths.get(entryFiltersDirectory).toAbsolutePath();
        log.info("Searching for entry filters in {}", path);

        EntryFilter.EntryFilterDefinitions entryFilterDefinitions = new EntryFilter.EntryFilterDefinitions();
        if (!path.toFile().exists()) {
            log.warn("Pulsar entry filters directory not found");
            return entryFilterDefinitions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    EntryFilter.EntryFilterDefinition def =
                            getEntryFilterDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found entry filter from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getEntryFilterClass()));

                    EntryFilter.EntryFilterMetaData metadata = new EntryFilter.EntryFilterMetaData();
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

    private static EntryFilter.EntryFilterDefinition getEntryFilterDefinition(String narPath,
                                                                              String narExtractionDirectory)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet(),
                narExtractionDirectory)) {
            return getEntryFilterDefinition(ncl);
        }
    }

    private static EntryFilter.EntryFilterDefinition getEntryFilterDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(
                configStr, EntryFilter.EntryFilterDefinition.class
        );
    }

    private static EntryFilter.EntryFilterWithClassLoader load(EntryFilter.EntryFilterMetaData metadata,
                                                               String narExtractionDirectory)
            throws IOException {
        NarClassLoader ncl = NarClassLoader.getFromArchive(
                metadata.getArchivePath().toAbsolutePath().toFile(),
                Collections.emptySet(),
                BrokerInterceptor.class.getClassLoader(), narExtractionDirectory);

        EntryFilter.EntryFilterDefinition def = getEntryFilterDefinition(ncl);
        if (StringUtils.isBlank(def.getEntryFilterClass())) {
            throw new IOException("Entry filters `" + def.getName() + "` does NOT provide a broker"
                    + " interceptors implementation");
        }

        try {
            Class entryFilterClass = ncl.loadClass(def.getEntryFilterClass());
            Object filter = entryFilterClass.getDeclaredConstructor().newInstance();
            if (!(filter instanceof EntryFilter)) {
                throw new IOException("Class " + def.getEntryFilterClass()
                        + " does not implement entry filter interface");
            }
            EntryFilter pi = (EntryFilter) filter;
            return new EntryFilter.EntryFilterWithClassLoader(pi, ncl);
        } catch (Throwable t) {
            return null;
        }
    }
}
