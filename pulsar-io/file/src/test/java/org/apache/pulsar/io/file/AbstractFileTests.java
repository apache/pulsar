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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class AbstractFileTests {

    protected BlockingQueue<File> workQueue;
    protected BlockingQueue<File> inProcess;
    protected BlockingQueue<File> recentlyProcessed;
    protected BlockingQueue<File> producedFiles;

    protected TestFileGenerator generatorThread;
    protected FileListingThread listingThread;
    protected ExecutorService executor;

    protected Path directory;

    @BeforeMethod(alwaysRun = true)
    public void init() throws IOException {
        // Create the directory we are going to read from
        directory = Files.createTempDirectory("pulsar-io-file-tests", getPermissions());

        workQueue = Mockito.spy(new LinkedBlockingQueue<>());
        inProcess = Mockito.spy(new LinkedBlockingQueue<>());
        recentlyProcessed = Mockito.spy(new LinkedBlockingQueue<>());
        producedFiles = Mockito.spy(new LinkedBlockingQueue<>());
        executor = Executors.newFixedThreadPool(10);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        // Shutdown all of the processing threads
        stopThreads();

        // Delete the directory and all the files
        cleanUp();
    }

    protected final void cleanUp() throws IOException {
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
           @Override
           public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
               Files.delete(file);
               return FileVisitResult.CONTINUE;
           }

           @Override
           public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
               Files.delete(dir);
               return FileVisitResult.CONTINUE;
           }
        });
    }

    protected void stopThreads() throws Exception {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    protected final void generateFiles(int numFiles) throws IOException, InterruptedException, ExecutionException {
        generateFiles(numFiles, 1, directory.toString());
    }

    protected final void generateFiles(int numFiles, int numLines) throws IOException, InterruptedException, ExecutionException {
        generateFiles(numFiles, numLines, directory.toString());
    }

    protected final void generateFiles(int numFiles, int numLines, String directory) throws IOException, InterruptedException, ExecutionException {
        generatorThread = new TestFileGenerator(producedFiles, numFiles, 1, numLines, directory, "prefix", ".txt", getPermissions());
        Future<?> f = executor.submit(generatorThread);
        f.get();
    }

    protected static final FileAttribute<Set<PosixFilePermission>> getPermissions() {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        return PosixFilePermissions.asFileAttribute(perms);
    }

}
