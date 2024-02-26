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
package org.apache.pulsar.functions.worker;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

@Slf4j
public class ConnectorsManager implements AutoCloseable {

    @Getter
    private volatile TreeMap<String, Connector> connectors;

    @VisibleForTesting
    public ConnectorsManager() {
        this.connectors = new TreeMap<>();
    }

    public ConnectorsManager(WorkerConfig workerConfig) throws IOException {
        this.connectors = createConnectors(workerConfig);
    }

    private static TreeMap<String, Connector> createConnectors(WorkerConfig workerConfig) throws IOException {
        boolean enableClassloading = workerConfig.getEnableClassloadingOfBuiltinFiles()
                || ThreadRuntimeFactory.class.getName().equals(workerConfig.getFunctionRuntimeFactoryClassName());
        return ConnectorUtils.searchForConnectors(workerConfig.getConnectorsDirectory(),
                workerConfig.getNarExtractionDirectory(), enableClassloading);
    }

    @VisibleForTesting
    public void addConnector(String connectorType, Connector connector) {
        connectors.put(connectorType, connector);
    }

    public Connector getConnector(String connectorType) {
        return connectors.get(connectorType);
    }

    public ConnectorDefinition getConnectorDefinition(String connectorType) {
        return connectors.get(connectorType).getConnectorDefinition();
    }

    public List<ConnectorDefinition> getConnectorDefinitions() {
        return connectors.values().stream().map(connector -> connector.getConnectorDefinition())
                .collect(Collectors.toList());
    }

    public Path getSourceArchive(String sourceType) {
        return connectors.get(sourceType).getArchivePath();
    }

    public List<ConfigFieldDefinition> getSourceConfigDefinition(String sourceType) {
        return connectors.get(sourceType).getSourceConfigFieldDefinitions();
    }

    public List<ConfigFieldDefinition> getSinkConfigDefinition(String sinkType) {
        return connectors.get(sinkType).getSinkConfigFieldDefinitions();
    }

    public Path getSinkArchive(String sinkType) {
        return connectors.get(sinkType).getArchivePath();
    }

    public void reloadConnectors(WorkerConfig workerConfig) throws IOException {
        TreeMap<String, Connector> oldConnectors = connectors;
        this.connectors = createConnectors(workerConfig);
        closeConnectors(oldConnectors);
    }

    @Override
    public void close() {
        closeConnectors(connectors);
    }

    private void closeConnectors(TreeMap<String, Connector> connectorMap) {
        connectorMap.values().forEach(connector -> {
            try {
                connector.close();
            } catch (Exception e) {
                log.warn("Failed to close connector", e);
            }
        });
        connectorMap.clear();
    }

}
