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
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.zeroturnaround.zip.ZipUtil;


@UtilityClass
@Slf4j
public class FunctionUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Extract the Pulsar Function class from a function or archive.
     */
    public static String getFunctionClass(File narFile) throws IOException {
        return getFunctionDefinition(narFile).getFunctionClass();
    }

    public static FunctionDefinition getFunctionDefinition(File narFile) throws IOException {
        return getPulsarIOServiceConfig(narFile, FunctionDefinition.class);
    }

    public static <T> T getPulsarIOServiceConfig(File narFile, Class<T> valueType) throws IOException {
        String filename = "META-INF/services/" + PULSAR_IO_SERVICE_NAME;
        byte[] configEntry = ZipUtil.unpackEntry(narFile, filename);
        if (configEntry != null) {
            return ObjectMapperFactory.getThreadLocalYaml().reader().readValue(configEntry, valueType);
        } else {
            return null;
        }
    }

    public static String getFunctionClass(NarClassLoader narClassLoader) throws IOException {
        return getFunctionDefinition(narClassLoader).getFunctionClass();
    }

    public static FunctionDefinition getFunctionDefinition(NarClassLoader narClassLoader) throws IOException {
        return getPulsarIOServiceConfig(narClassLoader, FunctionDefinition.class);
    }

    public static <T> T getPulsarIOServiceConfig(NarClassLoader narClassLoader, Class<T> valueType) throws IOException {
        return ObjectMapperFactory.getThreadLocalYaml().reader()
                .readValue(narClassLoader.getServiceDefinition(PULSAR_IO_SERVICE_NAME), valueType);
    }

    public static TreeMap<String, FunctionArchive> searchForFunctions(String functionsDirectory,
                                                                      String narExtractionDirectory,
                                                                      boolean enableClassloading) throws IOException {
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
                    FunctionDefinition cntDef = FunctionUtils.getFunctionDefinition(archive.toFile());
                    log.info("Found function {} from {}", cntDef, archive);
                    if (!StringUtils.isEmpty(cntDef.getFunctionClass())) {
                        FunctionArchive functionArchive =
                                new FunctionArchive(archive, cntDef, narExtractionDirectory, enableClassloading);
                        functions.put(cntDef.getName(), functionArchive);
                    }
                } catch (Throwable t) {
                    log.warn("Failed to load function from {}", archive, t);
                }
            }
        }

        return functions;
    }
}
