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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;

/**
 * Creates a temporary directory that contains 3 subdirectories,
 * "narExtractionDirectory", "downloadDirectory" and "connectorsDirectory",
 * which are assigned to the provided workerConfig's respective settings with
 * the {@link #useTemporaryDirectoriesForWorkerConfig(WorkerConfig)} method
 */
public class PulsarFunctionTestTemporaryDirectory {
    private final File tempDirectory;
    private final File narExtractionDirectory;
    private final File downloadDirectory;
    private final File connectorsDirectory;

    private PulsarFunctionTestTemporaryDirectory(String tempDirectoryNamePrefix) throws IOException {
        tempDirectory = Files.createTempDirectory(tempDirectoryNamePrefix).toFile();
        narExtractionDirectory = new File(tempDirectory, "narExtractionDirectory");
        narExtractionDirectory.mkdir();
        downloadDirectory = new File(tempDirectory, "downloadDirectory");
        downloadDirectory.mkdir();
        connectorsDirectory = new File(tempDirectory, "connectorsDirectory");
        connectorsDirectory.mkdir();
    }

    public static PulsarFunctionTestTemporaryDirectory create(String tempDirectoryNamePrefix) {
        try {
            return new PulsarFunctionTestTemporaryDirectory(tempDirectoryNamePrefix);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create temporary directory", e);
        }
    }

    public void useTemporaryDirectoriesForWorkerConfig(WorkerConfig workerConfig) {
        workerConfig.setNarExtractionDirectory(narExtractionDirectory.getAbsolutePath());
        workerConfig.setDownloadDirectory(downloadDirectory.getAbsolutePath());
        workerConfig.setConnectorsDirectory(connectorsDirectory.getAbsolutePath());
    }

    public void delete() {
        try {
            FileUtils.deleteDirectory(tempDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot delete temporary directory", e);
        }
    }

    public void assertThatFunctionDownloadTempFilesHaveBeenDeleted() {
        // make sure all temp files are deleted
        File[] foundFiles = downloadDirectory.listFiles((dir1, name) -> name.startsWith("function"));
        Assert.assertEquals(foundFiles.length, 0, "Temporary files left over: "
                + Arrays.asList(foundFiles));
    }
}
