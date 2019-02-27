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
import java.io.FileFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Worker thread that checks the configured input directory for
 * files that meet the provided filtering criteria, and publishes
 * them to a work queue for processing by the FileConsumerThreads.
 */
public class FileListingThread extends Thread {

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);
    private final Lock listingLock = new ReentrantLock();
    private final AtomicReference<FileFilter> fileFilterRef = new AtomicReference<>();
    private final BlockingQueue<File> workQueue;
    private final BlockingQueue<File> inProcess;
    private final BlockingQueue<File> recentlyProcessed;

    private final String inputDir;
    private final boolean recurseDirs;
    private final boolean keepOriginal;
    private final long pollingInterval;

    public FileListingThread(FileSourceConfig fileConfig,
            BlockingQueue<File> workQueue,
            BlockingQueue<File> inProcess,
            BlockingQueue<File> recentlyProcessed) {
        this.workQueue = workQueue;
        this.inProcess = inProcess;
        this.recentlyProcessed = recentlyProcessed;

        inputDir = fileConfig.getInputDirectory();
        recurseDirs = Optional.ofNullable(fileConfig.getRecurse()).orElse(true);
        keepOriginal = Optional.ofNullable(fileConfig.getKeepFile()).orElse(false);
        pollingInterval = Optional.ofNullable(fileConfig.getPollingInterval()).orElse(10000L);
        fileFilterRef.set(createFileFilter(fileConfig));
    }

    public void run() {
        while (true) {
            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingInterval) && listingLock.tryLock()) {
                try {
                    final File directory = new File(inputDir);
                    final Set<File> listing = performListing(directory, fileFilterRef.get(), recurseDirs);

                    if (listing != null && !listing.isEmpty()) {

                        // Remove any files that have been or are currently being processed.
                        listing.removeAll(inProcess);
                        if (!keepOriginal) {
                            listing.removeAll(recentlyProcessed);
                        }

                        for (File f: listing) {
                            if (!workQueue.contains(f)) {
                                workQueue.offer(f);
                            }
                        }
                        queueLastUpdated.set(System.currentTimeMillis());
                    }

                 } finally {
                    listingLock.unlock();
                 }
            }

            try {
                sleep(pollingInterval - 1);
            } catch (InterruptedException e) {
                // Just ignore
            }
        }
    }

    private Set<File> performListing(final File directory, final FileFilter filter,
            final boolean recurseSubdirectories) {
        Path p = directory.toPath();
        if (!Files.isWritable(p) || !Files.isReadable(p)) {
            throw new IllegalStateException("Directory '" + directory
                    + "' does not have sufficient permissions (i.e., not writable and readable)");
        }
        final Set<File> queue = new HashSet<>();
        if (!directory.exists()) {
            return queue;
        }

        final File[] children = directory.listFiles();
        if (children == null) {
            return queue;
        }

        for (final File child : children) {
            if (child.isDirectory()) {
                if (recurseSubdirectories) {
                    queue.addAll(performListing(child, filter, recurseSubdirectories));
                }
            } else if (filter.accept(child)) {
                queue.add(child);
            }
        }

        return queue;
    }

    private FileFilter createFileFilter(FileSourceConfig fileConfig) {
        final long minSize = Optional.ofNullable(fileConfig.getMinimumSize()).orElse(1);
        final Double maxSize = Optional.ofNullable(fileConfig.getMaximumSize()).orElse(Double.MAX_VALUE);
        final long minAge = Optional.ofNullable(fileConfig.getMinimumFileAge()).orElse(0);
        final Long maxAge = Optional.ofNullable(fileConfig.getMaximumFileAge()).orElse(Long.MAX_VALUE);
        final boolean ignoreHidden = Optional.ofNullable(fileConfig.getIgnoreHiddenFiles()).orElse(true);
        final Pattern filePattern = Pattern.compile(Optional.ofNullable(fileConfig.getFileFilter()).orElse("[^\\.].*"));
        final String indir = fileConfig.getInputDirectory();
        final String pathPatternStr = fileConfig.getPathFilter();
        final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);

        return new FileFilter() {
            @Override
            public boolean accept(final File file) {
                if (minSize > file.length()) {
                    return false;
                }
                if (maxSize != null && maxSize < file.length()) {
                    return false;
                }
                final long fileAge = System.currentTimeMillis() - file.lastModified();
                if (minAge > fileAge) {
                    return false;
                }
                if (maxAge != null && maxAge < fileAge) {
                    return false;
                }
                if (ignoreHidden && file.isHidden()) {
                    return false;
                }
                if (pathPattern != null) {
                    Path reldir = Paths.get(indir).relativize(file.toPath()).getParent();
                    if (reldir != null && !reldir.toString().isEmpty()) {
                        if (!pathPattern.matcher(reldir.toString()).matches()) {
                            return false;
                        }
                    }
                }
                //Verify that we have at least read permissions on the file we're considering grabbing
                if (!Files.isReadable(file.toPath())) {
                    return false;
                }

                /* Verify that if we're not keeping original that we have write
                 * permissions on the directory the file is in
                 */
                if (!keepOriginal && !Files.isWritable(file.toPath().getParent())) {
                    return false;
                }
                return filePattern.matcher(file.getName()).matches();
            }
        };
    }
}
