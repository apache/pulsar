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
package org.apache.pulsar.broker.intercept;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Util class to search and load {@link BrokerInterceptor}s.
 */
@UtilityClass
@Slf4j
public class BrokerInterceptorUtils {

    static final String BROKER_INTERCEPTOR_DEFINITION_FILE = "broker_interceptor.yml";

    /**
     * Retrieve the broker interceptor definition from the provided handler nar package.
     *
     * @param narPath the path to the broker interceptor NAR package
     * @return the broker interceptor definition
     * @throws IOException when fail to load the broker interceptor or get the definition
     */
    public BrokerInterceptorDefinition getBrokerInterceptorDefinition(String narPath, String narExtractionDirectory)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build()) {
            return getBrokerInterceptorDefinition(ncl);
        }
    }

    private BrokerInterceptorDefinition getBrokerInterceptorDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(BROKER_INTERCEPTOR_DEFINITION_FILE);

        return ObjectMapperFactory.getYamlMapper().reader().readValue(
                configStr, BrokerInterceptorDefinition.class
        );
    }

    /**
     * Search and load the available broker interceptors.
     *
     * @param interceptorsDirectory the directory where all the broker interceptors are stored
     * @return a collection of broker interceptors
     * @throws IOException when fail to load the available broker interceptors from the provided directory.
     */
    public BrokerInterceptorDefinitions searchForInterceptors(String interceptorsDirectory,
                                                              String narExtractionDirectory) throws IOException {
        Path path = Paths.get(interceptorsDirectory).toAbsolutePath();
        log.info("Searching for broker interceptors in {}", path);

        BrokerInterceptorDefinitions interceptors = new BrokerInterceptorDefinitions();
        if (!path.toFile().exists()) {
            log.warn("Pulsar broker interceptors directory not found");
            return interceptors;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    BrokerInterceptorDefinition def =
                            BrokerInterceptorUtils.getBrokerInterceptorDefinition(archive.toString(),
                                    narExtractionDirectory);
                    log.info("Found broker interceptors from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getInterceptorClass()));

                    BrokerInterceptorMetadata metadata = new BrokerInterceptorMetadata();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    interceptors.interceptors().put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load broker interceptor from {}."
                            + " It is OK however if you want to use this broker interceptor,"
                            + " please make sure you put the correct broker interceptor NAR"
                            + " package in the broker interceptors directory.", archive, t);
                }
            }
        }

        return interceptors;
    }

    /**
     * Load the broker interceptors according to the interceptor definition.
     *
     * @param metadata the broker interceptors definition.
     */
    BrokerInterceptorWithClassLoader load(BrokerInterceptorMetadata metadata, String narExtractionDirectory)
            throws IOException {
        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(BrokerInterceptorUtils.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();

        BrokerInterceptorDefinition def = getBrokerInterceptorDefinition(ncl);
        if (StringUtils.isBlank(def.getInterceptorClass())) {
            throw new IOException("Broker interceptors `" + def.getName() + "` does NOT provide a broker"
                    + " interceptors implementation");
        }

        try {
            Class interceptorClass = ncl.loadClass(def.getInterceptorClass());
            Object interceptor = interceptorClass.getDeclaredConstructor().newInstance();
            if (!(interceptor instanceof BrokerInterceptor)) {
                throw new IOException("Class " + def.getInterceptorClass()
                        + " does not implement broker interceptor interface");
            }
            BrokerInterceptor pi = (BrokerInterceptor) interceptor;
            return new BrokerInterceptorWithClassLoader(pi, ncl);
        } catch (Throwable t) {
            rethrowIOException(t);
            return null;
        }
    }

    private void rethrowIOException(Throwable cause)
            throws IOException {
        if (cause instanceof IOException) {
            throw (IOException) cause;
        } else if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
            throw (Error) cause;
        } else {
            throw new IOException(cause.getMessage(), cause);
        }
    }
}
