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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.nar.FileUtils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.zeroturnaround.zip.ZipUtil;


@UtilityClass
@CustomLog
public class FunctionUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Computes MD5 digest of a file as lower-case hex (for function archive identity on reload).
     */
    public static String computeArchiveMd5Hex(Path path) throws IOException {
        return calculateMd5Hex(path.toAbsolutePath().normalize().toFile());
    }

    private static String calculateMd5Hex(File file) throws IOException {
        return HexFormat.of().formatHex(FileUtils.calculateMd5sum(file));
    }

    /**
     * Extract the Pulsar Function class from a function or archive.
     */
    public static String getFunctionClass(File narFile) throws IOException {
        return getFunctionDefinition(narFile).getFunctionClass();
    }

    public static FunctionDefinition getFunctionDefinition(File narFile) throws IOException {
        return getPulsarIOServiceConfig(narFile, FunctionDefinition.class);
    }

    public static <T> T getPulsarIOServiceConfig(File narFile, Class<T> valueType) throws IOException {
        String filename = "META-INF/services/" + PULSAR_IO_SERVICE_NAME;
        byte[] configEntry = ZipUtil.unpackEntry(narFile, filename);
        if (configEntry != null) {
            return ObjectMapperFactory.getYamlMapper().reader().readValue(configEntry, valueType);
        } else {
            return null;
        }
    }

    public static String getFunctionClass(NarClassLoader narClassLoader) throws IOException {
        return getFunctionDefinition(narClassLoader).getFunctionClass();
    }

    public static FunctionDefinition getFunctionDefinition(NarClassLoader narClassLoader) throws IOException {
        return getPulsarIOServiceConfig(narClassLoader, FunctionDefinition.class);
    }

    public static <T> T getPulsarIOServiceConfig(NarClassLoader narClassLoader, Class<T> valueType) throws IOException {
        return ObjectMapperFactory.getYamlMapper().reader()
                .readValue(narClassLoader.getServiceDefinition(PULSAR_IO_SERVICE_NAME), valueType);
    }

    public static Map<String, FunctionArchive> searchForFunctions(String functionsDirectory,
                                                                      String narExtractionDirectory,
                                                                      boolean enableClassloading) throws IOException {
        Path path = Paths.get(functionsDirectory).toAbsolutePath().normalize();
        log.info().attr("path", path).log("Searching for functions");

        TreeMap<String, FunctionArchive> functions = new TreeMap<>();

        if (!path.toFile().exists()) {
            log.warn("Functions archive directory not found");
            return functions;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    FunctionDefinition cntDef = FunctionUtils.getFunctionDefinition(archive.toFile());
                    log.info().attr("function", cntDef).attr("archive", archive)
                            .log("Found function");
                    if (!StringUtils.isEmpty(cntDef.getFunctionClass())) {
                        FunctionArchive functionArchive =
                                new FunctionArchive(archive, cntDef, narExtractionDirectory, enableClassloading);
                        functions.put(cntDef.getName(), functionArchive);
                    }
                } catch (Throwable t) {
                    log.warn().attr("archive", archive).exception(t)
                            .log("Failed to load function");
                }
            }
        }

        return functions;
    }

    /**
     * Reloads functions from disk against {@code previous}, reusing {@link FunctionArchive} instances when path and
     * archive MD5 are unchanged (keeps class loaders open). New or changed archives get new instances.
     * <p>
     * {@link ReloadFunctionsResult#functionsToClose()} lists function archives evicted from the active set (replaced
     * or no longer present on disk); the caller must {@link FunctionArchive#close()} each.
     *
     * @param previous functions from the previous scan (may be empty, never null)
     * @param functionsDirectory same semantics as {@link #searchForFunctions}
     * @param narExtractionDirectory same semantics as {@link #searchForFunctions}
     * @param enableClassloading same semantics as {@link #searchForFunctions}
     * @return new map keyed by function name (reused values are identical instances from {@code previous}) and
     *         functions the caller should close
     */
    public static ReloadFunctionsResult reloadFunctions(
            Map<String, FunctionArchive> previous,
            String functionsDirectory,
            String narExtractionDirectory,
            boolean enableClassloading) throws IOException {

        TreeMap<String, FunctionArchive> remaining = new TreeMap<>(previous);
        TreeMap<String, FunctionArchive> next = new TreeMap<>();
        List<FunctionArchive> toClose = new ArrayList<>();

        Path dir = Paths.get(functionsDirectory).toAbsolutePath().normalize();
        if (!dir.toFile().exists()) {
            toClose.addAll(remaining.values());
            return new ReloadFunctionsResult(next, toClose);
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.nar")) {
            for (Path archive : stream) {
                try {
                    FunctionDefinition funcDef = FunctionUtils.getFunctionDefinition(archive.toFile());
                    if (!StringUtils.isEmpty(funcDef.getFunctionClass())) {
                        String name = funcDef.getName();
                        String md5Hex = computeArchiveMd5Hex(archive);
                        FunctionArchive prev = remaining.remove(name);
                        if (prev != null
                                && prev.getArchivePath() != null
                                && archive.equals(prev.getArchivePath())
                                && md5Hex.equals(prev.getArchiveMd5Hex())) {
                            next.put(name, prev);
                        } else {
                            if (prev != null) {
                                log.info()
                                        .attr("function", name)
                                        .attr("archive", archive)
                                        .attr("previousArchive", prev.getArchivePath())
                                        .log("Reloading changed function");
                                toClose.add(prev);
                            }
                            next.put(name, new FunctionArchive(archive, funcDef, narExtractionDirectory,
                                    enableClassloading, md5Hex));
                        }
                    }
                } catch (Throwable t) {
                    log.warn()
                            .attr("archive", archive)
                            .exception(t)
                            .log("Failed to load function");
                }
            }
        }
        toClose.addAll(remaining.values());
        return new ReloadFunctionsResult(next, toClose);
    }
}
