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
public class FileConsumerThreadTests extends AbstractFileTests {

    private PushSource<byte[]> consumer;
    private FileConsumerThread consumerThread;

    @Test
    public final void singleFileTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());

        try {
            generateFiles(1);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
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
            verify(consumer, times(1)).consume((Record<byte[]>) any(Record.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void mulitpleFileTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());

        try {
            generateFiles(50, 2);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
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
            verify(consumer, times(100)).consume((Record<byte[]>) any(Record.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }

    @Test
    public final void multiLineFileTest() throws IOException {

        consumer = Mockito.mock(PushSource.class);
        Mockito.doNothing().when(consumer).consume((Record<byte[]>) any(Record.class));

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", directory.toString());

        try {
            generateFiles(1, 10);
            listingThread = new FileListingThread(FileSourceConfig.load(map), workQueue, inProcess, recentlyProcessed);
            consumerThread = new FileConsumerThread(consumer, workQueue, inProcess, recentlyProcessed);
            executor.execute(listingThread);
            executor.execute(consumerThread);
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
            verify(consumer, times(10)).consume((Record<byte[]>) any(Record.class));
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to generate files" + e.getLocalizedMessage());
        }
    }
}
