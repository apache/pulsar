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
package org.apache.pulsar.functions.utils.functions;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.utils.Exceptions;


@UtilityClass
@Slf4j
public class FunctionUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Extract the Pulsar Function class from a function or archive.
     */
    public static String getFunctionClass(ClassLoader classLoader) throws IOException {
        NarClassLoader ncl = (NarClassLoader) classLoader;
        String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        FunctionDefinition conf = ObjectMapperFactory.getYamlMapper().reader().readValue(configStr,
        FunctionDefinition.class);
        if (StringUtils.isEmpty(conf.getFunctionClass())) {
            throw new IOException(
                    String.format("The '%s' functionctor does not provide a function implementation", conf.getName()));
        }

        try {
            // Try to load source class and check it implements Function interface
            Class functionClass = ncl.loadClass(conf.getFunctionClass());
            if (!(Function.class.isAssignableFrom(functionClass))) {
                throw new IOException(
                        "Class " + conf.getFunctionClass() + " does not implement interface " + Function.class
                                .getName());
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getFunctionClass();
    }

    public static FunctionDefinition getFunctionDefinition(NarClassLoader narClassLoader) throws IOException {
        String configStr = narClassLoader.getServiceDefinition(PULSAR_IO_SERVICE_NAME);
        return ObjectMapperFactory.getYamlMapper().reader().readValue(configStr, FunctionDefinition.class);
    }

    public static TreeMap<String, FunctionArchive> searchForFunctions(String functionsDirectory) throws IOException {
        return searchForFunctions(functionsDirectory, false);
    }

    public static TreeMap<String, FunctionArchive> searchForFunctions(String functionsDirectory,
                                                                      boolean alwaysPopulatePath) throws IOException {
        Path path = Paths.get(functionsDirectory).toAbsolutePath();
        log.info("Searching for functions in {}", path);

        TreeMap<String, FunctionArchive> functions = new TreeMap<>();

        if (!path.toFile().exists()) {
            log.warn("Functions archive directory not found");
            return functions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {

                    NarClassLoader ncl = NarClassLoaderBuilder.builder()
                            .narFile(new File(archive.toString()))
                            .build();

                    FunctionArchive.FunctionArchiveBuilder functionArchiveBuilder = FunctionArchive.builder();
                    FunctionDefinition cntDef = FunctionUtils.getFunctionDefinition(ncl);
                    log.info("Found function {} from {}", cntDef, archive);

                    functionArchiveBuilder.archivePath(archive);

                    functionArchiveBuilder.classLoader(ncl);
                    functionArchiveBuilder.functionDefinition(cntDef);

                    if (alwaysPopulatePath || !StringUtils.isEmpty(cntDef.getFunctionClass())) {
                        functions.put(cntDef.getName(), functionArchiveBuilder.build());
                    }
                } catch (Throwable t) {
                    log.warn("Failed to load function from {}", archive, t);
                }
            }
        }

        return functions;
    }
}
