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

/**
 * Pulsar broker interceptor.
 */
package org.apache.pulsar.proxy.server.protocol;

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
 * Util class to search and load {@link ProxyProtocols}.
 */
@UtilityClass
@Slf4j
public class ProxyProtocolUtils {

    final String PROXY_INTERCEPTOR_DEFINITION_FILE = "proxy_interceptor.yml";

    /**
     * Retrieve the proxy interceptor definition from the provided handler nar package.
     *
     * @param narPath the path to the proxy interceptor NAR package
     * @return the proxy interceptor definition
     * @throws IOException when fail to load the proxy interceptor or get the definition
     */
    public ProxyProtocolDefinition getProxyInterceptorDefinition(String narPath, String narExtractionDirectory) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet(), narExtractionDirectory)) {
            return getProxyInterceptorDefinition(ncl);
        }
    }

    private ProxyProtocolDefinition getProxyInterceptorDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(PROXY_INTERCEPTOR_DEFINITION_FILE);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(
                configStr, ProxyProtocolDefinition.class
        );
    }

    /**
     * Search and load the available proxy interceptors.
     *
     * @param interceptorsDirectory the directory where all the proxy interceptors are stored
     * @return a collection of proxy interceptors
     * @throws IOException when fail to load the available proxy interceptors from the provided directory.
     */
    public ProxyProtocolDefinitions searchForInterceptors(String interceptorsDirectory, String narExtractionDirectory) throws IOException {
        Path path = Paths.get(interceptorsDirectory).toAbsolutePath();
        log.info("Searching for proxy interceptors in {}", path);

        ProxyProtocolDefinitions interceptors = new ProxyProtocolDefinitions();
        if (!path.toFile().exists()) {
            log.warn("Pulsar proxy interceptors directory not found");
            return interceptors;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    ProxyProtocolDefinition def =
                            ProxyProtocolUtils.getProxyInterceptorDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found proxy interceptors from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getInterceptorClass()));

                    ProxyProtocolMetadata metadata = new ProxyProtocolMetadata();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    interceptors.interceptors().put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load proxy interceptor from {}."
                            + " It is OK however if you want to use this proxy interceptor,"
                            + " please make sure you put the correct proxy interceptor NAR"
                            + " package in the proxy interceptors directory.", archive, t);
                }
            }
        }

        return interceptors;
    }

    /**
     * Load the proxy interceptors according to the interceptor definition.
     *
     * @param metadata the proxy interceptors definition.
     */
    ProxyProtocolWithClassLoader load(ProxyProtocolMetadata metadata, String narExtractionDirectory) throws IOException {
        NarClassLoader ncl = NarClassLoader.getFromArchive(
                metadata.getArchivePath().toAbsolutePath().toFile(),
                Collections.emptySet(),
                ProxyProtocol.class.getClassLoader(), narExtractionDirectory);

        ProxyProtocolDefinition def = getProxyInterceptorDefinition(ncl);
        if (StringUtils.isBlank(def.getInterceptorClass())) {
            throw new IOException("Proxy interceptors `" + def.getName() + "` does NOT provide a proxy"
                    + " interceptors implementation");
        }

        try {
            Class interceptorClass = ncl.loadClass(def.getInterceptorClass());
            Object interceptor = interceptorClass.newInstance();
            if (!(interceptor instanceof ProxyProtocol)) {
                throw new IOException("Class " + def.getInterceptorClass()
                        + " does not implement proxy interceptor interface");
            }
            ProxyProtocol pi = (ProxyProtocol) interceptor;
            return new ProxyProtocolWithClassLoader(pi, ncl);
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
