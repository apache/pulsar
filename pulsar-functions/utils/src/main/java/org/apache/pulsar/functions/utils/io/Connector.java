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

import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.utils.FunctionFilePackage;
import org.apache.pulsar.functions.utils.ValidatableFunctionPackage;

public class Connector implements AutoCloseable {
    private final Path archivePath;
    private final String narExtractionDirectory;
    private final boolean enableClassloading;
    private ValidatableFunctionPackage connectorFunctionPackage;
    private List<ConfigFieldDefinition> sourceConfigFieldDefinitions;
    private List<ConfigFieldDefinition> sinkConfigFieldDefinitions;
    private ConnectorDefinition connectorDefinition;
    private boolean closed;

    public Connector(Path archivePath, ConnectorDefinition connectorDefinition, String narExtractionDirectory,
                     boolean enableClassloading) {
        this.archivePath = archivePath;
        this.connectorDefinition = connectorDefinition;
        this.narExtractionDirectory = narExtractionDirectory;
        this.enableClassloading = enableClassloading;
    }

    public Path getArchivePath() {
        return archivePath;
    }

    public synchronized ValidatableFunctionPackage getConnectorFunctionPackage() {
        checkState();
        if (connectorFunctionPackage == null) {
            connectorFunctionPackage =
                    new FunctionFilePackage(archivePath.toFile(), narExtractionDirectory, enableClassloading,
                            ConnectorDefinition.class);
        }
        return connectorFunctionPackage;
    }

    private void checkState() {
        if (closed) {
            throw new IllegalStateException("Connector is already closed");
        }
    }

    public synchronized List<ConfigFieldDefinition> getSourceConfigFieldDefinitions() {
        checkState();
        if (sourceConfigFieldDefinitions == null && !StringUtils.isEmpty(connectorDefinition.getSourceClass())
                && !StringUtils.isEmpty(connectorDefinition.getSourceConfigClass())) {
            sourceConfigFieldDefinitions = ConnectorUtils.getConnectorConfigDefinition(getConnectorFunctionPackage(),
                    connectorDefinition.getSourceConfigClass());
        }
        return sourceConfigFieldDefinitions;
    }

    public synchronized List<ConfigFieldDefinition> getSinkConfigFieldDefinitions() {
        checkState();
        if (sinkConfigFieldDefinitions == null && !StringUtils.isEmpty(connectorDefinition.getSinkClass())
                && !StringUtils.isEmpty(connectorDefinition.getSinkConfigClass())) {
            sinkConfigFieldDefinitions = ConnectorUtils.getConnectorConfigDefinition(getConnectorFunctionPackage(),
                    connectorDefinition.getSinkConfigClass());
        }
        return sinkConfigFieldDefinitions;
    }

    public ConnectorDefinition getConnectorDefinition() {
        return connectorDefinition;
    }

    @Override
    public synchronized void close() throws Exception {
        closed = true;
        if (connectorFunctionPackage instanceof AutoCloseable) {
            ((AutoCloseable) connectorFunctionPackage).close();
        }
    }
}
