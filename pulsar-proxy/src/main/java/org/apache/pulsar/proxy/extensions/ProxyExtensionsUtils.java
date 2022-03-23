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
package org.apache.pulsar.proxy.extensions;

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
 * Util class to search and load {@link ProxyExtension}s.
 */
@UtilityClass
@Slf4j
class ProxyExtensionsUtils {

    static final String PROXY_EXTENSION_DEFINITION_FILE = "pulsar-proxy-extension.yml";

    /**
     * Retrieve the extension definition from the provided handler nar package.
     *
     * @param narPath the path to the extension NAR package
     * @return the extension definition
     * @throws IOException when fail to load the extension or get the definition
     */
    public static ProxyExtensionDefinition getProxyExtensionDefinition(String narPath, String narExtractionDirectory)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build();) {
            return getProxyExtensionDefinition(ncl);
        }
    }

    private static ProxyExtensionDefinition getProxyExtensionDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(PROXY_EXTENSION_DEFINITION_FILE);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(
            configStr, ProxyExtensionDefinition.class
        );
    }

    /**
     * Search and load the available extensions.
     *
     * @param extensionsDirectory the directory where all the extensions are stored
     * @return a collection of extensions
     * @throws IOException when fail to load the available extensions from the provided directory.
     */
    public static ExtensionsDefinitions searchForExtensions(String extensionsDirectory,
                                                            String narExtractionDirectory) throws IOException {
        Path path = Paths.get(extensionsDirectory).toAbsolutePath();
        log.info("Searching for extensions in {}", path);

        ExtensionsDefinitions extensions = new ExtensionsDefinitions();
        if (!path.toFile().exists()) {
            log.warn("extension directory not found");
            return extensions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    ProxyExtensionDefinition phDef =
                        ProxyExtensionsUtils.getProxyExtensionDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found extension from {} : {}", archive, phDef);

                    checkArgument(StringUtils.isNotBlank(phDef.getName()));
                    checkArgument(StringUtils.isNotBlank(phDef.getExtensionClass()));

                    ProxyExtensionMetadata metadata = new ProxyExtensionMetadata();
                    metadata.setDefinition(phDef);
                    metadata.setArchivePath(archive);

                    extensions.extensions().put(phDef.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load connector from {}."
                        + " It is OK however if you want to use this extension,"
                        + " please make sure you put the correct extension NAR"
                        + " package in the extensions directory.", archive, t);
                }
            }
        }

        return extensions;
    }

    /**
     * Load the extension according to the handler definition.
     *
     * @param metadata the extension definition.
     * @return
     */
    static ProxyExtensionWithClassLoader load(ProxyExtensionMetadata metadata,
                                              String narExtractionDirectory) throws IOException {
        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(ProxyExtension.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();

        ProxyExtensionDefinition phDef = getProxyExtensionDefinition(ncl);
        if (StringUtils.isBlank(phDef.getExtensionClass())) {
            throw new IOException("extension `" + phDef.getName() + "` does NOT provide a protocol"
                + " handler implementation");
        }

        try {
            Class extensionClass = ncl.loadClass(phDef.getExtensionClass());
            Object extension = extensionClass.newInstance();
            if (!(extension instanceof ProxyExtension)) {
                throw new IOException("Class " + phDef.getExtensionClass()
                    + " does not implement extension interface");
            }
            ProxyExtension ph = (ProxyExtension) extension;
            return new ProxyExtensionWithClassLoader(ph, ncl);
        } catch (Throwable t) {
            rethrowIOException(t);
            return null;
        }
    }

    private static void rethrowIOException(Throwable cause)
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
