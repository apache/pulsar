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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

@Slf4j
public class ConnectorsManager {

    private final List<ConnectorDefinition> connectors = new ArrayList<>();
    private final Map<String, Path> sources = new TreeMap<>();
    private final Map<String, Path> sinks = new TreeMap<>();

    public ConnectorsManager(WorkerConfig workerConfig) throws IOException {
        searchForConnectors(workerConfig.getConnectorsDirectory());
    }

    public List<ConnectorDefinition> getConnectors() {
        return connectors;
    }

    public Path getSourceArchive(String sourceType) {
        return sources.get(sourceType);
    }

    public Path getSinkArchive(String sinkType) {
        return sinks.get(sinkType);
    }

    private void searchForConnectors(String connectorsDirectory) throws IOException {
        Path path = Paths.get(connectorsDirectory).toAbsolutePath();
        log.info("Searching for connectors in {}", path);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    ConnectorDefinition cntDef = ConnectorUtils.getConnectorDefinition(archive.toString());
                    log.info("Found connector {} from {}", cntDef, archive);

                    if (!StringUtils.isEmpty(cntDef.getSourceClass())) {
                        // Validate source class to be present and of the right type
                        ConnectorUtils.getIOSourceClass(archive.toString());
                        sources.put(cntDef.getName(), archive);
                    }

                    if (!StringUtils.isEmpty(cntDef.getSinkClass())) {
                        // Validate sinkclass to be present and of the right type
                        ConnectorUtils.getIOSinkClass(archive.toString());
                        sinks.put(cntDef.getName(), archive);
                    }

                    connectors.add(cntDef);
                } catch (Throwable t) {
                    log.warn("Failed to load connector from {}", archive, t);
                }
            }
        }

        Collections.sort(connectors, (c1, c2) -> String.CASE_INSENSITIVE_ORDER.compare(c1.getName(), c2.getName()));
    }
}
