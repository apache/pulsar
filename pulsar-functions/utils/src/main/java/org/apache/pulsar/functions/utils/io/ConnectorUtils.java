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
package org.apache.pulsar.functions.utils.io;

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
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;

@UtilityClass
@Slf4j
public class ConnectorUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Extract the Pulsar IO Source class from a connector archive.
     */
    public static String getIOSourceClass(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        ConnectorDefinition conf = ObjectMapperFactory.getThreadLocalYaml().readValue(configStr,
                ConnectorDefinition.class);
        if (StringUtils.isEmpty(conf.getSourceClass())) {
            throw new IOException(
                    String.format("The '%s' connector does not provide a source implementation", conf.getName()));
        }

        try {
            // Try to load source class and check it implements Source interface
            Class sourceClass = ncl.loadClass(conf.getSourceClass());
            if (!(Source.class.isAssignableFrom(sourceClass))) {
                throw new IOException("Class " + conf.getSourceClass() + " does not implement interface "
                        + Source.class.getName());
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getSourceClass();
    }

    /**
     * Extract the Pulsar IO Sink class from a connector archive.
     */
    public static String getIOSinkClass(ClassLoader classLoader) throws IOException {
        NarClassLoader ncl = (NarClassLoader) classLoader;
        String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        ConnectorDefinition conf = ObjectMapperFactory.getThreadLocalYaml().readValue(configStr,
                ConnectorDefinition.class);
        if (StringUtils.isEmpty(conf.getSinkClass())) {
            throw new IOException(
                    String.format("The '%s' connector does not provide a sink implementation", conf.getName()));
        }

        try {
            // Try to load source class and check it implements Sink interface
            Class sinkClass = ncl.loadClass(conf.getSinkClass());
            if (!(Sink.class.isAssignableFrom(sinkClass))) {
                throw new IOException(
                        "Class " + conf.getSinkClass() + " does not implement interface " + Sink.class.getName());
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getSinkClass();
    }

    public static ConnectorDefinition getConnectorDefinition(String narPath) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet())) {
            String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

            return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, ConnectorDefinition.class);
        }
    }

    public static Connectors searchForConnectors(String connectorsDirectory) throws IOException {
        Path path = Paths.get(connectorsDirectory).toAbsolutePath();
        log.info("Searching for connectors in {}", path);

        Connectors connectors = new Connectors();

        if (!path.toFile().exists()) {
            log.warn("Connectors archive directory not found");
            return connectors;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    ConnectorDefinition cntDef = ConnectorUtils.getConnectorDefinition(archive.toString());
                    log.info("Found connector {} from {}", cntDef, archive);

                    if (!StringUtils.isEmpty(cntDef.getSourceClass())) {
                        connectors.sources.put(cntDef.getName(), archive);
                    }

                    if (!StringUtils.isEmpty(cntDef.getSinkClass())) {
                        connectors.sinks.put(cntDef.getName(), archive);
                    }

                    connectors.connectors.add(cntDef);
                } catch (Throwable t) {
                    log.warn("Failed to load connector from {}", archive, t);
                }
            }
        }

        Collections.sort(connectors.connectors,
                (c1, c2) -> String.CASE_INSENSITIVE_ORDER.compare(c1.getName(), c2.getName()));

        return connectors;
    }
}
