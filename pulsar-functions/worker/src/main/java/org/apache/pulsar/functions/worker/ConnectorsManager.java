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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.utils.io.Connectors;

public class ConnectorsManager {

    private Connectors connectors;

    public ConnectorsManager(WorkerConfig workerConfig) throws IOException {
        this.connectors = ConnectorUtils.searchForConnectors(workerConfig.getConnectorsDirectory());
    }

    public List<ConnectorDefinition> getConnectors() {
        return connectors.getConnectors();
    }

    public Path getSourceArchive(String sourceType) {
        return connectors.getSources().get(sourceType);
    }

    public Path getSinkArchive(String sinkType) {
        return connectors.getSinks().get(sinkType);
    }

    public void reloadConnectors(WorkerConfig workerConfig) throws IOException {
        this.connectors = ConnectorUtils.searchForConnectors(workerConfig.getConnectorsDirectory());
    }
}
