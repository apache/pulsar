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
package org.apache.pulsar.broker.web.plugin.servlet;

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
 * Util class to search and load {@link AdditionalServlets}.
 */
@UtilityClass
@Slf4j
public class AdditionalServletUtils {

    public static final String ADDITIONAL_SERVLET_FILE = "additional_servlet.yml";

    /**
     * Retrieve the additional servlet definition from the provided nar package.
     *
     * @param narPath the path to the additional servlet NAR package
     * @return the additional servlet definition
     * @throws IOException when fail to load the additional servlet or get the definition
     */
    public AdditionalServletDefinition getAdditionalServletDefinition(
            String narPath, String narExtractionDirectory) throws IOException {
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build();) {
            return getAdditionalServletDefinition(ncl);
        }
    }

    private AdditionalServletDefinition getAdditionalServletDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(ADDITIONAL_SERVLET_FILE);
        return ObjectMapperFactory.getYamlMapper().reader().readValue(
                configStr, AdditionalServletDefinition.class
        );
    }

    /**
     * Search and load the available additional servlets.
     *
     * @param additionalServletDirectory the directory where all the additional servlets are stored
     * @return a collection of additional servlet definitions
     * @throws IOException when fail to load the available additional servlets from the provided directory.
     */
    public AdditionalServletDefinitions searchForServlets(String additionalServletDirectory,
                                                          String narExtractionDirectory) throws IOException {
        Path path = Paths.get(additionalServletDirectory).toAbsolutePath();
        log.info("Searching for additional servlets in {}", path);

        AdditionalServletDefinitions servletDefinitions = new AdditionalServletDefinitions();
        if (!path.toFile().exists()) {
            log.warn("Pulsar additional servlets directory not found");
            return servletDefinitions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    AdditionalServletDefinition def =
                            AdditionalServletUtils.getAdditionalServletDefinition(
                                    archive.toString(), narExtractionDirectory);
                    log.info("Found additional servlet from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getAdditionalServletClass()));

                    AdditionalServletMetadata metadata = new AdditionalServletMetadata();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    servletDefinitions.servlets().put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load additional servlet from {}."
                            + " It is OK however if you want to use this additional servlet,"
                            + " please make sure you put the correct additional servlet NAR"
                            + " package in the additional servlets directory.", archive, t);
                }
            }
        }

        return servletDefinitions;
    }

    /**
     * Load the additional servlets according to the additional servlet definition.
     *
     * @param metadata the additional servlet definition.
     */
    public AdditionalServletWithClassLoader load(
            AdditionalServletMetadata metadata, String narExtractionDirectory) throws IOException {

        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(AdditionalServlet.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();

        AdditionalServletDefinition def = getAdditionalServletDefinition(ncl);
        if (StringUtils.isBlank(def.getAdditionalServletClass())) {
            throw new IOException("Additional servlets `" + def.getName() + "` does NOT provide an "
                    + "additional servlets implementation");
        }

        try {
            Class additionalServletClass = ncl.loadClass(def.getAdditionalServletClass());
            Object additionalServlet = additionalServletClass.getDeclaredConstructor().newInstance();
            if (!(additionalServlet instanceof AdditionalServlet)) {
                throw new IOException("Class " + def.getAdditionalServletClass()
                        + " does not implement additional servlet interface");
            }
            AdditionalServlet servlet = (AdditionalServlet) additionalServlet;
            return new AdditionalServletWithClassLoader(servlet, ncl);
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
