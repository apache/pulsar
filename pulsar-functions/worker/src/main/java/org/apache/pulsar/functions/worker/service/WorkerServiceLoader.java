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
package org.apache.pulsar.functions.worker.service;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;

/**
 * Loader to load worker service.
 */
@Slf4j
public class WorkerServiceLoader {

    static final String PULSAR_FN_WORKER_DEFINITION_FILE = "pulsar-functions-worker-service.yml";

    /**
     * Retrieve the functions worker service definition from the provided worker service nar package.
     *
     * @param narPath the path to the worker service NAR package
     * @return the worker service definition
     * @throws IOException when fail to load the worker service or get the definition
     */
    public static WorkerServiceDefinition getWorkerServiceDefinition(String narPath, String narExtractionDirectory)
        throws IOException {
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build();) {
            return getWorkerServiceDefinition(ncl);
        }
    }

    private static WorkerServiceDefinition getWorkerServiceDefinition(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(PULSAR_FN_WORKER_DEFINITION_FILE);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(
            configStr, WorkerServiceDefinition.class
        );
    }

    /**
     * Load the worker service according to the worker service definition.
     *
     * @param metadata the worker service definition.
     * @return
     */
    static WorkerServiceWithClassLoader load(WorkerServiceMetadata metadata,
                                             String narExtractionDirectory) throws IOException {
        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(WorkerService.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();

        WorkerServiceDefinition phDef = getWorkerServiceDefinition(ncl);
        if (StringUtils.isBlank(phDef.getHandlerClass())) {
            throw new IOException("Functions Worker Service Nar Package `" + phDef.getName()
                + "` does NOT provide a functions worker service implementation");
        }

        try {
            Class handlerClass = ncl.loadClass(phDef.getHandlerClass());
            Object handler = handlerClass.getDeclaredConstructor().newInstance();
            if (!(handler instanceof WorkerService)) {
                throw new IOException("Class " + phDef.getHandlerClass()
                    + " does not implement worker service interface");
            }
            WorkerService ph = (WorkerService) handler;
            return new WorkerServiceWithClassLoader(ph, ncl);
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

    /**
     * Load the worker services for the given <tt>protocol</tt> list.
     *
     * @param workerConfig the functions worker config
     * @return the worker service
     */
    public static WorkerService load(WorkerConfig workerConfig) {
        return load(
            workerConfig.getFunctionsWorkerServiceNarPackage(),
            workerConfig.getNarExtractionDirectory());
    }

    /**
     * Load the worker services for the given <tt>protocol</tt> list.
     *
     * @param wsNarPackage worker service nar package
     * @param narExtractionDirectory the directory to extract nar directory
     * @return the worker service
     */
    static WorkerService load(String wsNarPackage, String narExtractionDirectory) {
        if (isEmpty(wsNarPackage)) {
            return new PulsarWorkerService();
        }

        WorkerServiceDefinition definition;
        try {
            definition = getWorkerServiceDefinition(
                wsNarPackage,
                narExtractionDirectory
            );
        } catch (IOException ioe) {
            log.error("Failed to get the worker service definition from {}",
                wsNarPackage, ioe);
            throw new RuntimeException("Failed to get the worker service definition from "
                + wsNarPackage, ioe);
        }

        WorkerServiceMetadata metadata = new WorkerServiceMetadata();
        Path narPath = Paths.get(wsNarPackage);
        metadata.setArchivePath(narPath);
        metadata.setDefinition(definition);

        WorkerServiceWithClassLoader service;
        try {
            service = load(metadata, narExtractionDirectory);
        } catch (IOException e) {
            log.error("Failed to load the worker service {}", metadata, e);
            throw new RuntimeException("Failed to load the worker service " + metadata, e);
        }

        log.info("Successfully loaded worker service {}", metadata);
        return service;
    }
}
