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
package org.apache.bookkeeper.mledger.offload;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Utils to load offloaders.
 */
@Slf4j
public class OffloaderUtils {

    private static final String PULSAR_OFFLOADER_SERVICE_NAME = "pulsar-offloader.yaml";

    /**
     * Extract the Pulsar offloader class from a offloader archive.
     *
     * @param narPath nar package path
     * @return the offloader class name
     * @throws IOException when fail to retrieve the pulsar offloader class
     */
    static Pair<NarClassLoader, LedgerOffloaderFactory> getOffloaderFactory(String narPath,
                                                                            String narExtractionDirectory)
            throws IOException {
        // need to load offloader NAR to the classloader that also loaded LedgerOffloaderFactory in case
        // LedgerOffloaderFactory is loaded by a classloader that is not the default classloader
        // as is the case for the pulsar presto plugin
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .parentClassLoader(LedgerOffloaderFactory.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();
        String configStr = ncl.getServiceDefinition(PULSAR_OFFLOADER_SERVICE_NAME);

        OffloaderDefinition conf = ObjectMapperFactory.getThreadLocalYaml()
            .readValue(configStr, OffloaderDefinition.class);
        if (StringUtils.isEmpty(conf.getOffloaderFactoryClass())) {
            throw new IOException(
                String.format("The '%s' offloader does not provide an offloader factory implementation",
                    conf.getName()));
        }

        try {
            // Try to load offloader factory class and check it implements Offloader interface
            Class factoryClass = ncl.loadClass(conf.getOffloaderFactoryClass());
            CompletableFuture<LedgerOffloaderFactory> loadFuture = new CompletableFuture<>();
            Thread loadingThread = new Thread(() -> {
                Thread.currentThread().setContextClassLoader(ncl);
                try {
                    Object offloader = factoryClass.getDeclaredConstructor().newInstance();
                    if (!(offloader instanceof LedgerOffloaderFactory)) {
                        throw new IOException("Class " + conf.getOffloaderFactoryClass() + " does not implement "
                                + "interface " + LedgerOffloaderFactory.class.getName());
                    }
                    loadFuture.complete((LedgerOffloaderFactory) offloader);
                } catch (Throwable t) {
                    loadFuture.completeExceptionally(t);
                }
            }, "load-factory-" + factoryClass);
            try {
                loadingThread.start();
                return Pair.of(ncl, loadFuture.get());
            } finally {
                loadingThread.join();
            }
        } catch (Throwable t) {
            rethrowIOException(t);
        }
        return null;
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

    public static OffloaderDefinition getOffloaderDefinition(String narPath, String narExtractionDirectory)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .parentClassLoader(LedgerOffloaderFactory.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build()) {
            String configStr = ncl.getServiceDefinition(PULSAR_OFFLOADER_SERVICE_NAME);

            return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, OffloaderDefinition.class);
        }
    }

    public static Offloaders searchForOffloaders(String offloadersPath, String narExtractionDirectory)
            throws IOException {
        Path path = Paths.get(offloadersPath).toAbsolutePath();
        log.info("Searching for offloaders in {}", path);

        Offloaders offloaders = new Offloaders();

        if (!path.toFile().exists()) {
            log.warn("Offloaders archive directory not found");
            return offloaders;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            stream.forEach(archive -> {
                try {
                    OffloaderDefinition definition = getOffloaderDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found offloader {} from {}", definition, archive);

                    if (!StringUtils.isEmpty(definition.getOffloaderFactoryClass())) {
                        // Validate offloader factory class to be present and of the right type
                        Pair<NarClassLoader,  LedgerOffloaderFactory> offloaderFactoryPair =
                            getOffloaderFactory(archive.toString(), narExtractionDirectory);
                        if (null != offloaderFactoryPair) {
                            offloaders.getOffloaders().add(offloaderFactoryPair);
                        }
                    }
                } catch (Throwable t) {
                    log.warn("Failed to load offloader from {}", archive, t);
                }
            });
        }
        log.info("Found and loaded {} offloaders", offloaders.getOffloaders().size());
        return offloaders;
    }


}
