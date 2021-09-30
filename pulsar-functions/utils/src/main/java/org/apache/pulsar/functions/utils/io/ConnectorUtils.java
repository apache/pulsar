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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@UtilityClass
@Slf4j
public class ConnectorUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Extract the Pulsar IO Source class from a connector archive.
     */
    public static String getIOSourceClass(NarClassLoader narClassLoader) throws IOException {
        ConnectorDefinition conf = getConnectorDefinition(narClassLoader);
        if (StringUtils.isEmpty(conf.getSourceClass())) {
            throw new IOException(
                    String.format("The '%s' connector does not provide a source implementation", conf.getName()));
        }

        try {
            // Try to load source class and check it implements Source interface
            Class sourceClass = narClassLoader.loadClass(conf.getSourceClass());
            if (!(Source.class.isAssignableFrom(sourceClass) || BatchSource.class.isAssignableFrom(sourceClass))) {
                throw new IOException(String.format("Class %s does not implement interface %s or %s",
                  conf.getSourceClass(), Source.class.getName(), BatchSource.class.getName()));
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getSourceClass();
    }

    /**
     * Extract the Pulsar IO Sink class from a connector archive.
     */
    public static String getIOSinkClass(NarClassLoader narClassLoader) throws IOException {
        ConnectorDefinition conf = getConnectorDefinition(narClassLoader);
        if (StringUtils.isEmpty(conf.getSinkClass())) {
            throw new IOException(
                    String.format("The '%s' connector does not provide a sink implementation", conf.getName()));
        }

        try {
            // Try to load sink class and check it implements Sink interface
            Class sinkClass = narClassLoader.loadClass(conf.getSinkClass());
            if (!(Sink.class.isAssignableFrom(sinkClass))) {
                throw new IOException(
                        "Class " + conf.getSinkClass() + " does not implement interface " + Sink.class.getName());
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getSinkClass();
    }

    public static ConnectorDefinition getConnectorDefinition(NarClassLoader narClassLoader) throws IOException {
        String configStr = narClassLoader.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, ConnectorDefinition.class);
    }

    public static List<ConfigFieldDefinition> getConnectorConfigDefinition(ClassLoader classLoader,
                                                                           String configClassName) throws Exception {
        List<ConfigFieldDefinition> retval = new LinkedList<>();
        Class configClass = classLoader.loadClass(configClassName);
        for (Field field : Reflections.getAllFields(configClass)) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                // We dont want static fields
                continue;
            }
            field.setAccessible(true);
            ConfigFieldDefinition configFieldDefinition = new ConfigFieldDefinition();
            configFieldDefinition.setFieldName(field.getName());
            configFieldDefinition.setTypeName(field.getType().getName());
            Map<String, String> attributes = new HashMap<>();
            for (Annotation annotation : field.getAnnotations()) {
                if (annotation.annotationType().equals(FieldDoc.class)) {
                    FieldDoc fieldDoc = (FieldDoc) annotation;
                    for (Method method : FieldDoc.class.getDeclaredMethods()) {
                        Object value = method.invoke(fieldDoc);
                        attributes.put(method.getName(), value == null ? "" : value.toString());
                    }
                }
            }
            configFieldDefinition.setAttributes(attributes);
            retval.add(configFieldDefinition);
        }

        return retval;
    }

    public static TreeMap<String, Connector> searchForConnectors(String connectorsDirectory, String narExtractionDirectory) throws IOException {
        Path path = Paths.get(connectorsDirectory).toAbsolutePath();
        log.info("Searching for connectors in {}", path);

        TreeMap<String, Connector> connectors = new TreeMap<>();
        if (!path.toFile().exists()) {
            log.warn("Connectors archive directory not found");
            return connectors;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {

                    NarClassLoader ncl = NarClassLoader.getFromArchive(new File(archive.toString()), Collections.emptySet(), narExtractionDirectory);

                    Connector.ConnectorBuilder connectorBuilder = Connector.builder();
                    ConnectorDefinition cntDef = ConnectorUtils.getConnectorDefinition(ncl);
                    log.info("Found connector {} from {}", cntDef, archive);

                    connectorBuilder.archivePath(archive);
                    if (!StringUtils.isEmpty(cntDef.getSourceClass())) {
                        if (!StringUtils.isEmpty(cntDef.getSourceConfigClass())) {
                            connectorBuilder.sourceConfigFieldDefinitions(ConnectorUtils.getConnectorConfigDefinition(ncl, cntDef.getSourceConfigClass()));
                        }
                    }

                    if (!StringUtils.isEmpty(cntDef.getSinkClass())) {
                        if (!StringUtils.isEmpty(cntDef.getSinkConfigClass())) {
                            connectorBuilder.sinkConfigFieldDefinitions(ConnectorUtils.getConnectorConfigDefinition(ncl, cntDef.getSinkConfigClass()));
                        }
                    }

                    connectorBuilder.classLoader(ncl);
                    connectorBuilder.connectorDefinition(cntDef);
                    connectors.put(cntDef.getName(), connectorBuilder.build());
                } catch (Throwable t) {
                    log.warn("Failed to load connector from {}", archive, t);
                }
            }

            return connectors;
        }
    }
}
