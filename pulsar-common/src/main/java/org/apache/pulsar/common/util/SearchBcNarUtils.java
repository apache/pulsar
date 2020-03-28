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
package org.apache.pulsar.common.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Provider;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;

/**
 * Utils to load one Bouncy Castle Provider.
 *  Prefered:   `org.bouncycastle.jce.provider.BouncyCastleProvider`
 *  or:         `org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider`.
 */
@Slf4j
public class SearchBcNarUtils {
    private static final String BC_DEF_NAME = "bouncy-castle.yaml";

    /**
     * Extract the Bouncy Castle Provider class from a archive path.
     * Search the path, and should only have 1 nar in the path.
     *
     * @param loaderDirectory nar package path
     * @return the Bouncy Castle Provider class name
     * @throws IOException when fail to retrieve the pulsar offloader class
     */
    static Provider getBcProvider(String loaderDirectory) throws IOException {
        Path path = Paths.get(loaderDirectory).toAbsolutePath();
        log.info("Searching for Bouncy Castle Loader in {}", path);
        if (!path.toFile().exists()) {
            log.warn("Bouncy Castle Loader archive directory not found");
            return null;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            try {
                Iterator<Path> iterator = stream.iterator();
                String narPath = iterator.next().toString();

                NarClassLoader ncl = NarClassLoader.getFromArchive(
                        new File(narPath),
                        Collections.emptySet(),
                        BCLoader.class.getClassLoader());
                String configStr = ncl.getServiceDefinition(BC_DEF_NAME);

                BcNarDefinition nar = ObjectMapperFactory.getThreadLocalYaml()
                        .readValue(configStr, BcNarDefinition.class);

                if (StringUtils.isEmpty(nar.getBcLoaderClass())) {
                    throw new IOException(
                            String.format("The '%s' not provided a Bouncy Castle Loader in nar yaml file",
                                    nar.getName()));
                }

                Class loaderClass = ncl.loadClass(nar.getBcLoaderClass());
                CompletableFuture<Provider> loadFuture = new CompletableFuture<>();
                Thread loadingThread = new Thread(() -> {
                    Thread.currentThread().setContextClassLoader(ncl);
                    try {
                        Object loader = loaderClass.newInstance();
                        if (!(loader instanceof BCLoader)) {
                            throw new IOException("Class " + nar.getBcLoaderClass() + " not a impl of "
                                                  + BCLoader.class.getName());
                        }

                        Provider provider = ((BCLoader) loader).getProvider();
                        log.info("Found Bouncy Castle loader {} from {}, provider: {}",
                                loader.getClass().getCanonicalName(), path, provider.getName());
                        loadFuture.complete(provider);
                    } catch (Throwable t) {
                        log.error("Failed to load Bouncy Castle Provider ", t);
                        loadFuture.completeExceptionally(t);
                    }
                }, "load-factory-" + loaderClass);
                try {
                    loadingThread.start();
                    Provider ret = loadFuture.get();
                    if (iterator.hasNext()) {
                        throw new RuntimeException("Should only have 1 Bouncy Castle Provider nar provided");
                    }
                    return ret;
                } finally {
                    loadingThread.join();
                }
            } catch (Throwable t) {
                log.error("Failed to load Bouncy Castle Provider with error", t);
                throw new RuntimeException(t);
            }
        }
    }
}
