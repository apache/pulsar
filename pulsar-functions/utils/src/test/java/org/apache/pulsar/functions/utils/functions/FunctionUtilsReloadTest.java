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
package org.apache.pulsar.functions.utils.functions;

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
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

@Test
public class FunctionUtilsReloadTest {

    private static void closeEvicted(ReloadFunctionsResult reload) throws Exception {
        for (FunctionArchive functionArchive : reload.functionsToClose()) {
            functionArchive.close();
        }
    }

    private static void writeMinimalNar(Path narPath, FunctionDefinition def) throws IOException {
        byte[] yaml = ObjectMapperFactory.getYamlMapper().getObjectMapper().writeValueAsBytes(def);
        try (OutputStream os = Files.newOutputStream(narPath);
                ZipOutputStream zos = new ZipOutputStream(os)) {
            ZipEntry entry = new ZipEntry("META-INF/services/pulsar-io.yaml");
            zos.putNextEntry(entry);
            zos.write(yaml);
            zos.closeEntry();
        }
    }

    private static FunctionDefinition sampleDefinition(String name) {
        FunctionDefinition def = new FunctionDefinition();
        def.setName(name);
        def.setFunctionClass("org.example.Function");
        return def;
    }

    /**
     * Historical {@code FunctionsManager} reload replaced the whole map and closed every prior
     * {@link FunctionArchive}, even when NAR files were unchanged. A caller keeping a reference to the
     * pre-reload archive would then hit {@link IllegalStateException} on lazy use.
     * <p>
     * Incremental reload must evict nothing, reuse the same instance, and leave that instance usable
     * after the caller closes only {@link ReloadFunctionsResult#functionsToClose()}.
     */
    @Test
    public void reloadUnchangedNarEvictsNothingAndKeepsSameFunctionArchiveUsable() throws Exception {
        Path dir = Files.createTempDirectory("fn-reload-");
        Path nar = dir.resolve("f1.nar");
        writeMinimalNar(nar, sampleDefinition("f-one"));

        Map<String, FunctionArchive> first =
                FunctionUtils.searchForFunctions(dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        FunctionArchive functionArchive = first.get("f-one");
        functionArchive.getFunctionPackage();

        ReloadFunctionsResult reload = FunctionUtils.reloadFunctions(
                first, dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        assertTrue(reload.functionsToClose().isEmpty());
        closeEvicted(reload);
        Map<String, FunctionArchive> second = reload.functions();

        assertSame(second.get("f-one"), functionArchive);
        functionArchive.getFunctionPackage();
    }

    @Test
    public void reloadReopensFunctionArchiveWhenNarContentChanges() throws Exception {
        Path dir = Files.createTempDirectory("fn-reload-");
        Path nar = dir.resolve("f1.nar");
        writeMinimalNar(nar, sampleDefinition("f-one"));

        Map<String, FunctionArchive> first =
                FunctionUtils.searchForFunctions(dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        FunctionArchive before = first.get("f-one");

        FunctionDefinition updated = sampleDefinition("f-one");
        updated.setDescription("changed");
        writeMinimalNar(nar, updated);

        ReloadFunctionsResult reload = FunctionUtils.reloadFunctions(
                first, dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        closeEvicted(reload);
        Map<String, FunctionArchive> second = reload.functions();

        assertNotSame(second.get("f-one"), before);
        assertThrows(IllegalStateException.class, before::getFunctionPackage);
    }

    @Test
    public void reloadClosesFunctionArchivesRemovedFromDirectory() throws Exception {
        Path dir = Files.createTempDirectory("fn-reload-");
        Path nar1 = dir.resolve("a.nar");
        Path nar2 = dir.resolve("b.nar");
        writeMinimalNar(nar1, sampleDefinition("fn-a"));
        writeMinimalNar(nar2, sampleDefinition("fn-b"));

        Map<String, FunctionArchive> first =
                FunctionUtils.searchForFunctions(dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        FunctionArchive removed = first.get("fn-b");
        Files.delete(nar2);

        ReloadFunctionsResult reload = FunctionUtils.reloadFunctions(
                first, dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        closeEvicted(reload);
        Map<String, FunctionArchive> second = reload.functions();

        assertEquals(second.size(), 1);
        assertSame(second.get("fn-a"), first.get("fn-a"));
        assertThrows(IllegalStateException.class, removed::getFunctionPackage);
    }

    @Test
    public void reloadClosesAllFunctionArchivesWhenDirectoryIsMissing() throws Exception {
        Path dir = Files.createTempDirectory("fn-reload-");
        Path nar = dir.resolve("f1.nar");
        writeMinimalNar(nar, sampleDefinition("f-one"));

        Map<String, FunctionArchive> first =
                FunctionUtils.searchForFunctions(dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        FunctionArchive removed = first.get("f-one");
        Files.delete(nar);
        Files.delete(dir);

        ReloadFunctionsResult reload = FunctionUtils.reloadFunctions(
                first, dir.toString(), NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, false);
        closeEvicted(reload);

        assertTrue(reload.functions().isEmpty());
        assertThrows(IllegalStateException.class, removed::getFunctionPackage);
    }
}
