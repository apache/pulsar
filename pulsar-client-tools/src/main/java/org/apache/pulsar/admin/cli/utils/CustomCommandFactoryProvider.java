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
package org.apache.pulsar.admin.cli.utils;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class CustomCommandFactoryProvider {

    @VisibleForTesting
    static final String COMMAND_FACTORY_ENTRY = "command_factory";

    /**
     * create a Command Factory.
     */
    public static List<CustomCommandFactory> createCustomCommandFactories(
            Properties conf) throws IOException {
        String names = conf.getProperty("customCommandFactories", "");
        List<CustomCommandFactory> result = new ArrayList<>();
        if (names.isEmpty()) {
            // early exit
            return result;
        }

        String directory = conf.getProperty("cliExtensionsDirectory", "cliextensions");
        String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;
        CustomCommandFactoryDefinitions definitions = searchForCustomCommandFactories(directory,
                narExtractionDirectory);
        for (String name : names.split(",")) {
            CustomCommandFactoryMetaData metaData = definitions.getFactories().get(name);
            if (null == metaData) {
                throw new RuntimeException("No factory is found for name `" + name
                        + "`. Available names are : " + definitions.getFactories());
            }
            CustomCommandFactory factory = load(metaData, narExtractionDirectory);
            if (factory != null) {
                result.add(factory);
            }
            log.debug("Successfully loaded command factory for name `{}`", name);
        }
        return result;
    }

    private static CustomCommandFactoryDefinitions searchForCustomCommandFactories(String directory,
                                                                       String narExtractionDirectory)
            throws IOException {
        Path path = Paths.get(directory).toAbsolutePath();
        log.debug("Searching for command factories  in {}", path);

        CustomCommandFactoryDefinitions customCommandFactoryDefinitions = new CustomCommandFactoryDefinitions();
        if (!path.toFile().exists()) {
            log.error("Pulsar command factories directory not found");
            return customCommandFactoryDefinitions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    CustomCommandFactoryDefinition def =
                            getCustomCommandFactoryDefinition(archive.toString(), narExtractionDirectory);
                    log.debug("Found command factory from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getFactoryClass()));

                    CustomCommandFactoryMetaData metadata = new CustomCommandFactoryMetaData();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    customCommandFactoryDefinitions.getFactories().put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load command factories from {}."
                            + " It is OK however if you want to use this command factory,"
                            + " please make sure you put the correct NAR"
                            + " package in the directory.", archive, t);
                }
            }
        }

        return customCommandFactoryDefinitions;
    }

    private static CustomCommandFactoryDefinition getCustomCommandFactoryDefinition(String narPath,
                                                                                    String narExtractionDirectory)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build()) {
            return getCustomCommandFactoryDefinition(ncl);
        }
    }

    @VisibleForTesting
    static CustomCommandFactoryDefinition getCustomCommandFactoryDefinition(NarClassLoader ncl) throws IOException {
        String configStr;

        try {
            configStr = ncl.getServiceDefinition(COMMAND_FACTORY_ENTRY + ".yaml");
        } catch (NoSuchFileException e) {
            configStr = ncl.getServiceDefinition(COMMAND_FACTORY_ENTRY + ".yml");
        }

        return ObjectMapperFactory.getYamlMapper().reader().readValue(
                configStr, CustomCommandFactoryDefinition.class
        );
    }

    private static CustomCommandFactory load(CustomCommandFactoryMetaData metadata,
                                                   String narExtractionDirectory)
            throws IOException {
        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(CustomCommandFactory.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();
        CustomCommandFactoryDefinition def = getCustomCommandFactoryDefinition(ncl);
        if (StringUtils.isBlank(def.getFactoryClass())) {
            throw new IOException("Command Factory `" + def.getName() + "` does NOT provide a Command Factory"
                    + " implementation");
        }

        try {
            Class commandFactoryClass = ncl.loadClass(def.getFactoryClass());
            Object factory = commandFactoryClass.getDeclaredConstructor().newInstance();
            if (!(factory instanceof CustomCommandFactory)) {
                throw new IOException("Class " + def.getFactoryClass()
                        + " does not implement CustomCommandFactory interface");
            }
           return (CustomCommandFactory) factory;
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            log.error("Failed to load class {}", def.getFactoryClass(), e);
            throw new IOException(e);
        }
    }
}
