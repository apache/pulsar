/*
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

import static org.testng.Assert.assertSame;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.utils.io.Connector;
import org.testng.annotations.Test;

/**
 * Tests {@link ConnectorsManager#reloadConnectors(WorkerConfig)} for incremental reload behavior,
 * ensuring unchanged connectors are reused instead of being recreated.
 */
public class ConnectorsManagerReloadConnectorsTest {

    private static void writeMinimalNar(Path narPath, ConnectorDefinition def) throws IOException {
        byte[] yaml = ObjectMapperFactory.getYamlMapper().getObjectMapper().writeValueAsBytes(def);
        try (OutputStream os = Files.newOutputStream(narPath);
                ZipOutputStream zos = new ZipOutputStream(os)) {
            ZipEntry entry = new ZipEntry("META-INF/services/pulsar-io.yaml");
            zos.putNextEntry(entry);
            zos.write(yaml);
            zos.closeEntry();
        }
    }

    private static ConnectorDefinition sampleDefinition(String name) {
        ConnectorDefinition def = new ConnectorDefinition();
        def.setName(name);
        def.setSinkClass("org.example.Sink");
        def.setSourceClass("org.example.Source");
        return def;
    }

    @Test
    public void reloadWhenNarUnchangedReusesSameConnectorInstance() throws Exception {
        Path dir = Files.createTempDirectory("mgr-conn-reload-");
        Path nar = dir.resolve("c1.nar");
        writeMinimalNar(nar, sampleDefinition("c-one"));

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setConnectorsDirectory(dir.toString());
        workerConfig.setNarExtractionDirectory(NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR);
        workerConfig.setEnableClassloadingOfBuiltinFiles(false);

        try (ConnectorsManager manager = new ConnectorsManager(workerConfig)) {
            Connector before = manager.getConnector("c-one");
            before.getConnectorFunctionPackage();

            manager.reloadConnectors(workerConfig);

            Connector after = manager.getConnector("c-one");
            assertSame(after, before);
            before.getConnectorFunctionPackage();
        }
    }

}
