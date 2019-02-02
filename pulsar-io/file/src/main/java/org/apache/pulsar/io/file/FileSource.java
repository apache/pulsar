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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

/**
 * A simple connector to consume messages from the local file system.
 * It can be configured to consume files recursively from a given
 * directory, and can handle plain text, gzip, and zip formatted files.
 */
public class FileSource extends PushSource<byte[]> {

    private ExecutorService executor;
    private final BlockingQueue<File> workQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<File> inProcess = new LinkedBlockingQueue<>();
    private final BlockingQueue<File> recentlyProcessed = new LinkedBlockingQueue<>();

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        FileSourceConfig fileConfig = FileSourceConfig.load(config);
        fileConfig.validate();

        // One extra for the File listing thread, and another for the cleanup thread
        executor = Executors.newFixedThreadPool(fileConfig.getNumWorkers() + 2);
        executor.execute(new FileListingThread(fileConfig, workQueue, inProcess, recentlyProcessed));
        executor.execute(new ProcessedFileThread(fileConfig, recentlyProcessed));

        for (int idx = 0; idx < fileConfig.getNumWorkers(); idx++) {
            executor.execute(new FileConsumerThread(this, workQueue, inProcess, recentlyProcessed));
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
