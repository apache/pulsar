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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.file.utils.GZipFiles;
import org.apache.pulsar.io.file.utils.ZipFiles;

/**
 * Worker thread that consumes the contents of the files
 * and publishes them to a Pulsar topic.
 */
public class FileConsumerThread extends Thread {

    private final PushSource<byte[]> source;
    private final BlockingQueue<File> workQueue;
    private final BlockingQueue<File> inProcess;
    private final BlockingQueue<File> recentlyProcessed;

    public FileConsumerThread(PushSource<byte[]> source,
            BlockingQueue<File> workQueue,
            BlockingQueue<File> inProcess,
            BlockingQueue<File> recentlyProcessed) {
        this.source = source;
        this.workQueue = workQueue;
        this.inProcess = inProcess;
        this.recentlyProcessed = recentlyProcessed;
    }

    public void run() {
        try {
            while (true) {
                File file = workQueue.take();

                boolean added = false;
                do {
                    added = inProcess.add(file);
                } while (!added);

                consumeFile(file);
            }
        } catch (InterruptedException ie) {
            // just terminate
        }
    }

    private void consumeFile(File file) {
        final AtomicInteger idx = new AtomicInteger(1);
        try (Stream<String> lines = getLines(file)) {
             lines.forEachOrdered(line -> process(file, idx.getAndIncrement(), line));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            boolean removed = false;
            do {
                removed = inProcess.remove(file);
            } while (!removed);

            boolean added = false;
            do {
                added = recentlyProcessed.add(file);
            } while (!added);
        }
    }

    private Stream<String> getLines(File file) throws IOException {
        if (file == null) {
            return null;
        } else if (GZipFiles.isGzip(file)) {
            return GZipFiles.lines(Paths.get(file.getAbsolutePath()));
        } else if (ZipFiles.isZip(file)) {
            return ZipFiles.lines(Paths.get(file.getAbsolutePath()));
        } else {
            return Files.lines(Paths.get(file.getAbsolutePath()), Charset.defaultCharset());
        }
    }

    private void process(File srcFile, int lineNumber, String line) {
        source.consume(new FileRecord(srcFile, lineNumber, line.getBytes()));
    }

}
