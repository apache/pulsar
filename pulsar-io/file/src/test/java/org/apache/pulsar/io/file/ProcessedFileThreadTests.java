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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class ProcessedFileThreadTests extends AbstractFileTests {

    private PushSource<byte[]> consumer;
    private FileConsumerThread consumerThread;
    private ProcessedFileThread cleanupThread;
    private FileSourceConfig fileConfig;

    @Test
    public final void singleFileTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.FALSE);

        try {
            generateFiles(1);
            fileConfig = FileSourceConfig.load(map);
            listingThread = new FileListingThread(fileConfig, workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            cleanupThread = new ProcessedFileThread(fileConfig, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
            executor.execute(cleanupThread);
            Thread.sleep(2000);

            for (File produced : producedFiles) {
                verify(workQueue, times(1)).offer(produced);
                verify(inProcess, times(1)).add(produced);
                verify(inProcess, times(1)).remove(produced);
                verify(recentlyProcessed, times(1)).add(produced);
            }

            verify(workQueue, times(1)).offer(any(File.class));
            verify(workQueue, atLeast(1)).take();
            verify(inProcess, times(1)).add(any(File.class));
            verify(inProcess, times(1)).remove(any(File.class));
            verify(recentlyProcessed, times(1)).add(any(File.class));
            verify(recentlyProcessed, times(2)).take();
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void multipleFileTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.FALSE);

        try {
            generateFiles(50);
            fileConfig = FileSourceConfig.load(map);
            listingThread = new FileListingThread(fileConfig, workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            cleanupThread = new ProcessedFileThread(fileConfig, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
            executor.execute(cleanupThread);
            Thread.sleep(2000);

            for (File produced : producedFiles) {
                verify(workQueue, times(1)).offer(produced);
                verify(inProcess, times(1)).add(produced);
                verify(inProcess, times(1)).remove(produced);
                verify(recentlyProcessed, times(1)).add(produced);
            }

            verify(workQueue, times(50)).offer(any(File.class));
            verify(workQueue, atLeast(50)).take();
            verify(inProcess, times(50)).add(any(File.class));
            verify(inProcess, times(50)).remove(any(File.class));
            verify(recentlyProcessed, times(50)).add(any(File.class));
            verify(recentlyProcessed, times(50)).add(any(File.class));
            verify(recentlyProcessed, times(51)).take();
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void keepFileTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.TRUE);
        map.put("pollingInterval", 1000L);

        try {
            generateFiles(1);
            fileConfig = FileSourceConfig.load(map);
            listingThread = new FileListingThread(fileConfig, workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            cleanupThread = new ProcessedFileThread(fileConfig, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
            executor.execute(cleanupThread);
            Thread.sleep(7900);  // Should pull the same file 5 times?

            for (File produced : producedFiles) {
                verify(workQueue, atLeast(4)).offer(produced);
                verify(inProcess, atLeast(4)).add(produced);
                verify(inProcess, atLeast(4)).remove(produced);
                verify(recentlyProcessed, atLeast(4)).add(produced);
            }

            verify(recentlyProcessed, atLeast(5)).take();
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void continuousRunTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.FALSE);
        map.put("pollingInterval", 100);
        fileConfig = FileSourceConfig.load(map);

        try {
            // Start producing files, with a .1 sec delay between
            generatorThread = new TestFileGenerator(producedFiles, 5000, 100, 1, directory.toString(), "continuous", ".txt", getPermissions());
            executor.execute(generatorThread);

            listingThread = new FileListingThread(fileConfig, workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            cleanupThread = new ProcessedFileThread(fileConfig, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
            executor.execute(cleanupThread);

            // Run for 30 seconds
            Thread.sleep(30000);

            // Stop producing files
            generatorThread.halt();

            // Let the consumer catch up
            while (!workQueue.isEmpty() && !inProcess.isEmpty() && !recentlyProcessed.isEmpty()) {
                Thread.sleep(2000);
            }

            // Make sure every single file was processed.
            for (File produced : producedFiles) {
                verify(workQueue, times(1)).offer(produced);
                verify(inProcess, times(1)).add(produced);
                verify(inProcess, times(1)).remove(produced);
                verify(recentlyProcessed, times(1)).add(produced);
            }

        } catch (InterruptedException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void multipleConsumerTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.FALSE);
        map.put("pollingInterval", 100);
        fileConfig = FileSourceConfig.load(map);

        try {
            // Start producing files, with a .1 sec delay between
            generatorThread = new TestFileGenerator(producedFiles, 5000, 100, 1, directory.toString(), "continuous", ".txt", getPermissions());
            executor.execute(generatorThread);

            listingThread = new FileListingThread(fileConfig, workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            FileConsumerThread consumerThread2 = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            FileConsumerThread consumerThread3 = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            cleanupThread = new ProcessedFileThread(fileConfig, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
            executor.execute(consumerThread2);
            executor.execute(consumerThread3);
            executor.execute(cleanupThread);

            // Run for 30 seconds
            Thread.sleep(30000);

            // Stop producing files
            generatorThread.halt();

            // Let the consumer catch up
            while (!workQueue.isEmpty() && !inProcess.isEmpty() && !recentlyProcessed.isEmpty()) {
                Thread.sleep(2000);
            }

            // Make sure every single file was processed exactly once.
            for (File produced : producedFiles) {
                verify(workQueue, times(1)).offer(produced);
                verify(inProcess, times(1)).add(produced);
                verify(inProcess, times(1)).remove(produced);
                verify(recentlyProcessed, times(1)).add(produced);
            }

        } catch (InterruptedException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void renameFileTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        String processedFileSuffix = ".file_process_done";
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("inputDirectory", directory.toString());
        map.put("keepFile", Boolean.FALSE);
        map.put("pollingInterval", 100);
        map.put("processedFileSuffix", processedFileSuffix);
        fileConfig = FileSourceConfig.load(map);

        try {
            // Start producing files, with a .1 sec delay between
            generatorThread =
                    new TestFileGenerator(producedFiles, 500, 100, 1, directory.toString(), "continuous", ".txt",
                            getPermissions());
            executor.execute(generatorThread);

            listingThread = new FileListingThread(fileConfig, workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            cleanupThread = new ProcessedFileThread(fileConfig, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
            executor.execute(cleanupThread);

            // Run for 30 seconds
            Thread.sleep(30000);

            // Stop producing files
            generatorThread.halt();

            // Let the consumer catch up
            while (!workQueue.isEmpty() && !inProcess.isEmpty() && !recentlyProcessed.isEmpty()) {
                Thread.sleep(2000);
            }


            // Make sure every single file was processed.
            for (File produced : producedFiles) {
                verify(workQueue, times(1)).offer(produced);
                verify(inProcess, times(1)).add(produced);
                verify(inProcess, times(1)).remove(produced);
                verify(recentlyProcessed, times(1)).add(produced);

                assert(!produced.exists());
                assert(new File(produced.getAbsolutePath() + processedFileSuffix).exists());
            }

        } catch (InterruptedException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }
}
