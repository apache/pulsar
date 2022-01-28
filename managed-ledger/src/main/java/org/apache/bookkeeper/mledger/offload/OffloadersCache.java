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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of an Offloaders. The main purpose of this class is to
 * ensure that an Offloaders directory is only loaded once.
 */
@Slf4j
public class OffloadersCache implements AutoCloseable {

    private Map<String, Offloaders> loadedOffloaders = new ConcurrentHashMap<>();

    /**
     * Method to load an Offloaders directory or to get an already loaded Offloaders directory.
     *
     * @param offloadersPath - the directory to search the offloaders nar files
     * @param narExtractionDirectory - the directory to use for extraction
     * @return the loaded offloaders class
     * @throws IOException when fail to retrieve the pulsar offloader class
     */
    public Offloaders getOrLoadOffloaders(String offloadersPath, String narExtractionDirectory) {
        return loadedOffloaders.computeIfAbsent(offloadersPath,
                (directory) -> {
                    try {
                        return OffloaderUtils.searchForOffloaders(directory, narExtractionDirectory);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void close() {
        loadedOffloaders.values().forEach(offloaders -> {
            try {
                offloaders.close();
            } catch (Exception e) {
                log.error("Error while closing offloader.", e);
                // Even if the offloader fails to close, the graceful shutdown process continues
            }
        });
        // Don't want to hold on to references to closed offloaders
        loadedOffloaders.clear();
    }
}
