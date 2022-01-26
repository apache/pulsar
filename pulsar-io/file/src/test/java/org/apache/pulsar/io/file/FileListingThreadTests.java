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

import static org.testng.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.testng.annotations.Test;


public class FileListingThreadTests extends AbstractFileTests {

    @Test
    public final void singleFileTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());

        try {
            generateFiles(1);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(producedFiles, times(1)).put(any(File.class));
            verify(workQueue, times(1)).offer(any(File.class));

            for (File produced : producedFiles) {
                verify(workQueue, times(1)).offer(produced);
            }

        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void fiftyFileTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());

        try {
            generateFiles(50);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(50)).offer(any(File.class));

            for (File produced : producedFiles) {
                verify(workQueue, times(1)).offer(produced);
            }

        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void minimumSizeTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());

        try {
            // Create 50 zero size files
            generateFiles(50, 0);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(0)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void maximumSizeTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("maximumSize", "1000");

        try {
            // Create 5 files that exceed the limit and 45 that don't
            generateFiles(5, 1000);
            generateFiles(45, 10);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(45)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }

    @Test
    public final void minimumAgeTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("minimumFileAge", "5000");

        try {
            // Create 5 files that will be too "new" for processing
            generateFiles(5);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(0)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }

    @Test
    public final void maximumAgeTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("maximumFileAge", "5000");

        try {
            // Create 5 files that will be processed
            generateFiles(5);
            Thread.sleep(5000);

            // Create 5 files that will be too "old" for processing
            generateFiles(5);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(5)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }

    @Test
    public final void doRecurseTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("recurse", Boolean.TRUE);

        try {
            // Create 5 files in the root folder
            generateFiles(5);

            // Create 5 files in a sub-folder
            generateFiles(5, 1, directory.toString() + File.separator + "sub-dir");
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(10)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }

    @Test
    public final void doNotRecurseTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("recurse", Boolean.FALSE);

        try {
            // Create 5 files in the root folder
            generateFiles(5);

            // Create 5 files in a sub-folder
            generateFiles(5, 1, directory.toString() + File.separator + "sub-dir");
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(5)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }

    @Test
    public final void pathFilterTest() throws IOException {

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("recurse", Boolean.TRUE);
        map.put("pathFilter", "sub-.*");

        try {
            // Create 5 files in a sub-folder
            generateFiles(5, 1, directory.toString() + File.separator + "sub-dir-a");
            generateFiles(5, 1, directory.toString() + File.separator + "dir-b");
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(5)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }

    @Test
    public final void processedFileFilterTest() throws IOException {

        String processedFileSuffix = ".file_process_done";
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.FALSE);
        map.put("processedFileSuffix", processedFileSuffix);

        try {
            generateFiles(5, 1, directory.toString(), ".txt");
            generateFiles(1, 1, directory.toString(), processedFileSuffix);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(5)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }

    @Test
    public final void processedFileFilterTest2() throws IOException {

        String processedFileSuffix = ".file_process_done";
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.TRUE);
        map.put("processedFileSuffix", processedFileSuffix);

        try {
            generateFiles(5, 1, directory.toString(), ".txt");
            generateFiles(1, 1, directory.toString(), processedFileSuffix);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            Thread.sleep(2000);
            verify(workQueue, times(6)).offer(any(File.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        } finally {
            cleanUp();
        }
    }
}
