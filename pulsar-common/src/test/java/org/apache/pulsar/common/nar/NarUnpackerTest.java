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
package org.apache.pulsar.common.nar;

import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class NarUnpackerTest {
    File sampleZipFile;
    File extractDirectory;

    @BeforeMethod
    public void createSampleZipFile() throws IOException {
        sampleZipFile = Files.createTempFile("sample", ".zip").toFile();
        try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(sampleZipFile))) {
            for (int i = 0; i < 5000; i++) {
                ZipEntry e = new ZipEntry("hello" + i + ".txt");
                out.putNextEntry(e);
                byte[] msg = "hello world!".getBytes(StandardCharsets.UTF_8);
                out.write(msg, 0, msg.length);
                out.closeEntry();
            }
        }
        extractDirectory = Files.createTempDirectory("nar_unpack_dir").toFile();
    }

    @AfterMethod(alwaysRun = true)
    void deleteSampleZipFile() {
        if (sampleZipFile != null && sampleZipFile.exists()) {
            try {
                sampleZipFile.delete();
            } catch (Exception e) {
                log.warn("Failed to delete file {}", sampleZipFile, e);
            }
        }
        if (extractDirectory != null && extractDirectory.exists()) {
            try {
                FileUtils.deleteFile(extractDirectory, true);
            } catch (IOException e) {
                log.warn("Failed to delete directory {}", extractDirectory, e);
            }
        }
    }

    @Test
    void shouldExtractFilesOnceInSameProcess() throws InterruptedException {
        int threads = 20;
        CountDownLatch countDownLatch = new CountDownLatch(threads);
        AtomicInteger exceptionCounter = new AtomicInteger();
        AtomicInteger extractCounter = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    NarUnpacker.doUnpackNar(sampleZipFile, extractDirectory, extractCounter::incrementAndGet);
                } catch (Exception e) {
                    log.error("Unpacking failed", e);
                    exceptionCounter.incrementAndGet();
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(exceptionCounter.get(), 0);
        assertEquals(extractCounter.get(), 1);
    }

    public static class NarUnpackerWorker {
        public static void main(String[] args) {
            File sampleZipFile = new File(args[0]);
            File extractDirectory = new File(args[1]);
            AtomicInteger extractCounter = new AtomicInteger();
            try {
                NarUnpacker.doUnpackNar(sampleZipFile, extractDirectory, extractCounter::incrementAndGet);
                if (extractCounter.get() == 1) {
                    System.exit(101);
                } else if (extractCounter.get() == 0) {
                    System.exit(100);
                }
            } catch (Exception e) {
                log.error("Unpacking failed", e);
                System.exit(99);
            }
        }
    }

    @Test
    void shouldExtractFilesOnceInDifferentProcess() throws InterruptedException {
        int processes = 5;
        String javaExePath = findJavaExe().getAbsolutePath();
        CountDownLatch countDownLatch = new CountDownLatch(processes);
        AtomicInteger exceptionCounter = new AtomicInteger();
        AtomicInteger extractCounter = new AtomicInteger();
        for (int i = 0; i < processes; i++) {
            new Thread(() -> {
                try {
                    // fork a new process with the same classpath
                    Process process = new ProcessBuilder()
                            .command(javaExePath,
                                    "-Xmx96m",
                                    "-XX:TieredStopAtLevel=1",
                                    "-Dlog4j2.disable.jmx=true",
                                    "-cp",
                                    System.getProperty("java.class.path"),
                                    // use NarUnpackerWorker as the main class
                                    NarUnpackerWorker.class.getName(),
                                    // pass arguments to use for testing
                                    sampleZipFile.getAbsolutePath(),
                                    extractDirectory.getAbsolutePath())
                            .redirectErrorStream(true)
                            .start();
                    String output = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
                    int retval = process.waitFor();
                    log.info("Process retval {} output {}", retval, output);
                    if (retval == 101) {
                        extractCounter.incrementAndGet();
                    } else if (retval != 100) {
                        exceptionCounter.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Unpacking in a separate process failed", e);
                    exceptionCounter.incrementAndGet();
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(exceptionCounter.get(), 0);
        assertEquals(extractCounter.get(), 1);
    }

    File findJavaExe() {
        File javaHome = new File(System.getProperty("java.home"));
        File javaExe = new File(javaHome, "bin/java" + (SystemUtils.IS_OS_WINDOWS ? ".exe" : ""));
        return javaExe;
    }
}