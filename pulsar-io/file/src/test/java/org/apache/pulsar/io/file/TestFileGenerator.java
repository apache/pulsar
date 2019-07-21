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
package org.apache.pulsar.io.file;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;

public class TestFileGenerator extends Thread {

    // Allows us to communicate back which files we generated
    private final BlockingQueue<File> producedFiles;
    private final int numFiles;
    private final long delay;
    private final int numLines;
    private final String prefix;
    private final String suffix;
    private final FileAttribute<?>[] attrs;
    private final Path tempDir;
    private boolean keepRunning = true;
    
    public TestFileGenerator(BlockingQueue<File> producedFiles, int numFiles, long delay, int numLines, 
            String dir, String prefix, String suffix, FileAttribute<?>... attrs) throws IOException {
        this.numFiles = numFiles;
        this.delay = delay;
        this.numLines = numLines;
        this.producedFiles = producedFiles;
        this.prefix = prefix;
        this.suffix = suffix;
        this.attrs = attrs;
        tempDir = Files.createDirectories(Paths.get(dir), attrs);
    }
    
    public void run() {
        int counter = 0;
        while  ( keepRunning && (counter++ < numFiles)) {
            createFile();
            try {
                sleep(delay);
            } catch (InterruptedException e) {
                return;
            }
        }
    }
    
    public void halt() {
        keepRunning = false;
    }
    
    private final void createFile() {
        try {
            Path path = Files.createTempFile(tempDir, prefix, suffix, attrs);
            try(OutputStream out = Files.newOutputStream(path, StandardOpenOption.APPEND)) {
              for (int idx = 0; idx < numLines; idx++) {
                 IOUtils.write(RandomStringUtils.random(50, true, false) + "\n", out, "UTF-8");
              }
            }
            
            producedFiles.put(path.toFile());
            
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
