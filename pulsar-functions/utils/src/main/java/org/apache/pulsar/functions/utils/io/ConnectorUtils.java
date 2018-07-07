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
import java.util.Collections;

import lombok.experimental.UtilityClass;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;

@UtilityClass
public class ConnectorUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Extract the Pulsar IO Source class from a connector archive.
     */
    public static String getIOSourceClass(String narPath) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet())) {
            String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

            ConnectorDefinition conf = ObjectMapperFactory.getThreadLocalYaml().readValue(configStr,
                    ConnectorDefinition.class);
            if (StringUtils.isEmpty(conf.getSourceClass())) {
                throw new IOException(
                        String.format("The '%s' connector does not provide a source implementation", conf.getName()));
            }

            try {
                // Try to load source class and check it implements Source interface
                Object instance = ncl.loadClass(conf.getSourceClass()).newInstance();
                if (!(instance instanceof Source)) {
                    throw new IOException("Class " + conf.getSourceClass() + " does not implement interface "
                            + Source.class.getName());
                }
            } catch (Throwable t) {
                Exceptions.rethrowIOException(t);
            }

            return conf.getSourceClass();
        }
    }

    /**
     * Extract the Pulsar IO Sink class from a connector archive.
     */
    public static String getIOSinkClass(String narPath) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet())) {
            String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

            ConnectorDefinition conf = ObjectMapperFactory.getThreadLocalYaml().readValue(configStr,
                    ConnectorDefinition.class);
            if (StringUtils.isEmpty(conf.getSinkClass())) {
                throw new IOException(
                        String.format("The '%s' connector does not provide a sink implementation", conf.getName()));
            }

            try {
                // Try to load source class and check it implements Sink interface
                Object instance = ncl.loadClass(conf.getSinkClass()).newInstance();
                if (!(instance instanceof Sink)) {
                    throw new IOException(
                            "Class " + conf.getSinkClass() + " does not implement interface " + Sink.class.getName());
                }
            } catch (Throwable t) {
                Exceptions.rethrowIOException(t);
            }

            return conf.getSinkClass();
        }
    }

    public static ConnectorDefinition getConnectorDefinition(String narPath) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet())) {
            String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

            return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, ConnectorDefinition.class);
        }
    }
}
