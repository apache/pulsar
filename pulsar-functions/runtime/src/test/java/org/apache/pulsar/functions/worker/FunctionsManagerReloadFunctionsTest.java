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
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.utils.functions.FunctionArchive;
import org.testng.annotations.Test;

/**
 * Tests {@link FunctionsManager#reloadFunctions(WorkerConfig)} for incremental reload behavior,
 * ensuring unchanged functions are reused instead of being recreated.
 */
public class FunctionsManagerReloadFunctionsTest {

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

    @Test
    public void reloadWhenNarUnchangedReusesSameFunctionArchiveInstance() throws Exception {
        Path dir = Files.createTempDirectory("mgr-fn-reload-");
        Path nar = dir.resolve("f1.nar");
        writeMinimalNar(nar, sampleDefinition("f-one"));

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setFunctionsDirectory(dir.toString());
        workerConfig.setNarExtractionDirectory(NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR);
        workerConfig.setEnableClassloadingOfBuiltinFiles(false);

        try (FunctionsManager manager = new FunctionsManager(workerConfig)) {
            FunctionArchive before = manager.getFunction("f-one");
            before.getFunctionPackage();

            manager.reloadFunctions(workerConfig);

            FunctionArchive after = manager.getFunction("f-one");
            assertSame(after, before);
            before.getFunctionPackage();
        }
    }
}
