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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.lang3.StringUtils;

/**
 * Worker thread that cleans up all the files that have been processed.
 */
public class ProcessedFileThread extends Thread {

    private final BlockingQueue<File> recentlyProcessed;
    private final boolean keepOriginal;
    private final String processedFileSuffix;

    public ProcessedFileThread(FileSourceConfig fileConfig, BlockingQueue<File> recentlyProcessed) {
        keepOriginal = Optional.ofNullable(fileConfig.getKeepFile()).orElse(false);
        processedFileSuffix = fileConfig.getProcessedFileSuffix();
        this.recentlyProcessed = recentlyProcessed;
    }

    public void run() {
        try {
            while (true) {
                File file = recentlyProcessed.take();
                handle(file);
            }
        } catch (InterruptedException ie) {
            // just terminate
        }
    }

    private void handle(File f) {
        if (!keepOriginal) {
            try {
                if (StringUtils.isBlank(processedFileSuffix)) {
                    Files.deleteIfExists(f.toPath());
                } else {
                    File targetFile = new File(f.getParentFile(), f.getName() + processedFileSuffix);
                    Files.move(f.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
