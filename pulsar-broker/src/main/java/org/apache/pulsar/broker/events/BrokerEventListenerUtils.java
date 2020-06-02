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
package org.apache.pulsar.broker.events;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Util class to search and load {@link BrokerEventListener}s.
 */
@UtilityClass
@Slf4j
public class BrokerEventListenerUtils {

    static final String BROKER_LISTENER_DEFINITION_FILE = "broker_listener.yml";

    /**
     * Retrieve the broker listener definition from the provided handler nar package.
     *
     * @param narPath the path to the broker listener NAR package
     * @return the broker listener definition
     * @throws IOException when fail to load the broker listener or get the definition
     */
    public static BrokerEventListenerDefinition getBrokerListenerDefinition(String narPath, String narExtractionDirectory) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet(), narExtractionDirectory)) {
            return getBrokerListenerDefinition(ncl);
        }
    }

    private static BrokerEventListenerDefinition getBrokerListenerDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(BROKER_LISTENER_DEFINITION_FILE);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(
                configStr, BrokerEventListenerDefinition.class
        );
    }

    /**
     * Search and load the available broker listeners.
     *
     * @param listenersDirectory the directory where all the broker listeners are stored
     * @return a collection of broker listeners
     * @throws IOException when fail to load the available broker listeners from the provided directory.
     */
    public static BrokerEventListenerDefinitions searchForListeners(String listenersDirectory, String narExtractionDirectory) throws IOException {
        Path path = Paths.get(listenersDirectory).toAbsolutePath();
        log.info("Searching for broker listeners in {}", path);

        BrokerEventListenerDefinitions listeners = new BrokerEventListenerDefinitions();
        if (!path.toFile().exists()) {
            log.warn("Pulsar broker listeners directory not found");
            return listeners;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    BrokerEventListenerDefinition def =
                            BrokerEventListenerUtils.getBrokerListenerDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found broker listeners from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getListenerClass()));

                    BrokerEventListenerMetadata metadata = new BrokerEventListenerMetadata();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    listeners.listeners().put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load broker listener from {}."
                            + " It is OK however if you want to use this broker listener,"
                            + " please make sure you put the correct broker listener NAR"
                            + " package in the broker listeners directory.", archive, t);
                }
            }
        }

        return listeners;
    }

    /**
     * Load the broker listeners according to the listener definition.
     *
     * @param metadata the broker listeners definition.
     * @return
     */
    static SafeBrokerEventListenerWithClassLoader load(BrokerEventListenerMetadata metadata, String narExtractionDirectory) throws IOException {
        NarClassLoader ncl = NarClassLoader.getFromArchive(
                metadata.getArchivePath().toAbsolutePath().toFile(),
                Collections.emptySet(),
                BrokerEventListener.class.getClassLoader(), narExtractionDirectory);

        BrokerEventListenerDefinition def = getBrokerListenerDefinition(ncl);
        if (StringUtils.isBlank(def.getListenerClass())) {
            throw new IOException("Broker listeners `" + def.getName() + "` does NOT provide a broker"
                    + " listener implementation");
        }

        try {
            Class listenerClass = ncl.loadClass(def.getListenerClass());
            Object listener = listenerClass.newInstance();
            if (!(listener instanceof BrokerEventListener)) {
                throw new IOException("Class " + def.getListenerClass()
                        + " does not implement broker listener interface");
            }
            BrokerEventListener pi = (BrokerEventListener) listener;
            return new SafeBrokerEventListenerWithClassLoader(pi, ncl);
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
