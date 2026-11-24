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
package org.apache.pulsar.functions.utils.io;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

@Test
public class ConnectorUtilsReloadTest {

    private static void closeEvicted(ReloadConnectorsResult reload) throws Exception {
        for (Connector c : reload.connectorsToClose()) {
            c.close();
        }
    }

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

    /**
     * Historical {@code ConnectorsManager} reload replaced the whole map and closed every prior
     * {@link Connector}, even when NAR files were unchanged. A caller keeping a reference to the
     * pre-reload connector would then hit {@link IllegalStateException} on lazy use.
     * <p>
     * Incremental reload must evict nothing, reuse the same instance, and leave that instance usable
     * after the caller closes only {@link ReloadConnectorsResult#connectorsToClose()}.
     */
    @Test
    public void reloadUnchangedNarEvictsNothingAndKeepsSameConnectorUsable() throws Exception {
        Path dir = Files.createTempDirectory("conn-reload-");
        Path nar = dir.resolve("c1.nar");
        writeMinimalNar(nar, sampleDefinition("c-one"));

        Map<String, Connector> first =
                ConnectorUtils.searchForConnectors(dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        Connector c1 = first.get("c-one");
        c1.getConnectorFunctionPackage();

        ReloadConnectorsResult reload = ConnectorUtils.reloadConnectors(
                first, dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        assertTrue(reload.connectorsToClose().isEmpty());
        closeEvicted(reload);
        Map<String, Connector> second = reload.connectors();

        assertSame(second.get("c-one"), c1);
        c1.getConnectorFunctionPackage();
    }

    @Test
    public void reloadReopensConnectorWhenNarContentChanges() throws Exception {
        Path dir = Files.createTempDirectory("conn-reload-");
        Path nar = dir.resolve("c1.nar");
        writeMinimalNar(nar, sampleDefinition("c-one"));

        Map<String, Connector> first =
                ConnectorUtils.searchForConnectors(dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        Connector before = first.get("c-one");

        ConnectorDefinition updated = sampleDefinition("c-one");
        updated.setDescription("changed");
        writeMinimalNar(nar, updated);

        ReloadConnectorsResult reload = ConnectorUtils.reloadConnectors(
                first, dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        closeEvicted(reload);
        Map<String, Connector> second = reload.connectors();

        assertNotSame(second.get("c-one"), before);
        assertThrows(IllegalStateException.class, before::getConnectorFunctionPackage);
    }

    @Test
    public void reloadClosesConnectorsRemovedFromDirectory() throws Exception {
        Path dir = Files.createTempDirectory("conn-reload-");
        Path nar1 = dir.resolve("a.nar");
        Path nar2 = dir.resolve("b.nar");
        writeMinimalNar(nar1, sampleDefinition("conn-a"));
        writeMinimalNar(nar2, sampleDefinition("conn-b"));

        Map<String, Connector> first =
                ConnectorUtils.searchForConnectors(dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        Connector removed = first.get("conn-b");
        Files.delete(nar2);

        ReloadConnectorsResult reload = ConnectorUtils.reloadConnectors(
                first, dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        closeEvicted(reload);
        Map<String, Connector> second = reload.connectors();

        assertEquals(second.size(), 1);
        assertSame(second.get("conn-a"), first.get("conn-a"));
        assertThrows(IllegalStateException.class, removed::getConnectorFunctionPackage);
    }

}
