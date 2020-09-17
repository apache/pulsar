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
package org.apache.pulsar.proxy.server.plugin.servlet;

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
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Util class to search and load {@link ProxyAdditionalServlets}.
 */
@UtilityClass
@Slf4j
public class ProxyAdditionalServletUtils {

    public final String PROXY_ADDITIONAL_SERVLET_FILE = "proxy_additional_servlet.yml";

    /**
     * Retrieve the proxy additional servlet definition from the provided nar package.
     *
     * @param narPath the path to the proxy additional servlet NAR package
     * @return the proxy additional servlet definition
     * @throws IOException when fail to load the proxy additional servlet or get the definition
     */
    public ProxyAdditionalServletDefinition getProxyAdditionalServletDefinition(
            String narPath, String narExtractionDirectory) throws IOException {

        try (NarClassLoader ncl = NarClassLoader.getFromArchive(
                new File(narPath), Collections.emptySet(), narExtractionDirectory)) {
            return getProxyAdditionalServletDefinition(ncl);
        }
    }

    private ProxyAdditionalServletDefinition getProxyAdditionalServletDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(PROXY_ADDITIONAL_SERVLET_FILE);
        return ObjectMapperFactory.getThreadLocalYaml().readValue(
                configStr, ProxyAdditionalServletDefinition.class
        );
    }

    /**
     * Search and load the available proxy additional servlets.
     *
     * @param additionalServletDirectory the directory where all the proxy additional servlets are stored
     * @return a collection of proxy additional servlet definitions
     * @throws IOException when fail to load the available proxy additional servlets from the provided directory.
     */
    public ProxyAdditionalServletDefinitions searchForServlets(String additionalServletDirectory,
                                                               String narExtractionDirectory) throws IOException {
        Path path = Paths.get(additionalServletDirectory).toAbsolutePath();
        log.info("Searching for proxy additional servlets in {}", path);

        ProxyAdditionalServletDefinitions servletDefinitions = new ProxyAdditionalServletDefinitions();
        if (!path.toFile().exists()) {
            log.warn("Pulsar proxy additional servlets directory not found");
            return servletDefinitions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    ProxyAdditionalServletDefinition def =
                            ProxyAdditionalServletUtils.getProxyAdditionalServletDefinition(
                                    archive.toString(), narExtractionDirectory);
                    log.info("Found proxy additional servlet from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getAdditionalServletClass()));

                    ProxyAdditionalServletMetadata metadata = new ProxyAdditionalServletMetadata();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    servletDefinitions.servlets().put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load proxy additional servlet from {}."
                            + " It is OK however if you want to use this proxy additional servlet,"
                            + " please make sure you put the correct proxy additional servlet NAR"
                            + " package in the proxy additional servlets directory.", archive, t);
                }
            }
        }

        return servletDefinitions;
    }

    /**
     * Load the proxy additional servlets according to the additional servlet definition.
     *
     * @param metadata the proxy additional servlet definition.
     */
    public ProxyAdditionalServletWithClassLoader load(
            ProxyAdditionalServletMetadata metadata, String narExtractionDirectory) throws IOException {

        NarClassLoader ncl = NarClassLoader.getFromArchive(
                metadata.getArchivePath().toAbsolutePath().toFile(),
                Collections.emptySet(),
                ProxyAdditionalServlet.class.getClassLoader(), narExtractionDirectory);

        ProxyAdditionalServletDefinition def = getProxyAdditionalServletDefinition(ncl);
        if (StringUtils.isBlank(def.getAdditionalServletClass())) {
            throw new IOException("Proxy additional servlets `" + def.getName() + "` does NOT provide a proxy"
                    + " additional servlets implementation");
        }

        try {
            Class additionalServletClass = ncl.loadClass(def.getAdditionalServletClass());
            Object additionalServlet = additionalServletClass.newInstance();
            if (!(additionalServlet instanceof ProxyAdditionalServlet)) {
                throw new IOException("Class " + def.getAdditionalServletClass()
                        + " does not implement proxy additional servlet interface");
            }
            ProxyAdditionalServlet servlet = (ProxyAdditionalServlet) additionalServlet;
            return new ProxyAdditionalServletWithClassLoader(servlet, ncl);
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
