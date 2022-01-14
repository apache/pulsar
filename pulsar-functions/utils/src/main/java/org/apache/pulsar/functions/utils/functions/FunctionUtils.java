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
import java.util.Collections;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.functions.api.Function;


@UtilityClass
@Slf4j
public class FunctionUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Extract the Pulsar Function class from a functionctor archive.
     */
    public static String getFunctionClass(ClassLoader classLoader) throws IOException {
        NarClassLoader ncl = (NarClassLoader) classLoader;
        String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        FunctionDefinition conf = ObjectMapperFactory.getThreadLocalYaml().readValue(configStr,
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
                        "Class " + conf.getFunctionClass() + " does not implement interface " + Function.class.getName());
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getFunctionClass();
    }

    public static FunctionDefinition getFunctionDefinition(String narPath) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet())) {
            String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);
            return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, FunctionDefinition.class);
        }
    }
    public static Functions searchForFunctions(String functionsDirectory) throws IOException {
        return searchForFunctions(functionsDirectory, false);
    }

    public static Functions searchForFunctions(String functionsDirectory, boolean alwaysPopulatePath) throws IOException {
        Path path = Paths.get(functionsDirectory).toAbsolutePath();
        log.info("Searching for functions in {}", path);

        Functions functions = new Functions();

        if (!path.toFile().exists()) {
            log.warn("Functions archive directory not found");
            return functions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    FunctionDefinition cntDef = FunctionUtils.getFunctionDefinition(archive.toString());
                    log.info("Found function {} from {}", cntDef, archive);
                    log.error(cntDef.getName());
                    log.error(cntDef.getFunctionClass());
                    if (alwaysPopulatePath || !StringUtils.isEmpty(cntDef.getFunctionClass())) {
                        functions.functions.put(cntDef.getName(), archive);
                    }

                    functions.functionsDefinitions.add(cntDef);
                } catch (Throwable t) {
                    log.warn("Failed to load function from {}", archive, t);
                }
            }
        }

        Collections.sort(functions.functionsDefinitions,
                (c1, c2) -> String.CASE_INSENSITIVE_ORDER.compare(c1.getName(), c2.getName()));

        return functions;
    }
}
