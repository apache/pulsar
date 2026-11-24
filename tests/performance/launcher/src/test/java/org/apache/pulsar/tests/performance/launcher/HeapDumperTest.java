/*
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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

public class HeapDumperTest {
    @Test
    public void readsTheHeapUsageOfZgcAndG1() {
        // jcmd GC.heap_info of ZGC and of G1
        assertThat(HeapDumper.parseUsage("""
                88:
                 ZHeap            used 1904M, capacity 2048M, max capacity 2048M
                 Cache           144M (50)
                """)).isEqualTo(new HeapDumper.HeapUsage(1904L << 20, 2048L << 20));
        assertThat(HeapDumper.parseUsage("""
                71:
                 garbage-first heap   total reserved 524288K, committed 524288K, used 122160K [0x00000000e0000000, \
                0x0000000100000000)
                  region size 1024K, 45 young (46080K), 3 survivors (3072K)
                """)).isEqualTo(new HeapDumper.HeapUsage(122160L << 10, 524288L << 10));
        assertThat(HeapDumper.parseUsage("71:\n unknown collector\n")).isNull();
    }

    @Test
    public void dumpsAPeakOnlyWhenTheHeapIsHalfFullAndHasGrownByATenth() {
        long max = 4096L << 20;
        // The heap filling up while the cluster starts isn't a peak
        assertThat(HeapDumper.isNewPeak(new HeapDumper.HeapUsage(1024L << 20, max), 0)).isFalse();
        assertThat(HeapDumper.isNewPeak(new HeapDumper.HeapUsage(2048L << 20, max), 0)).isTrue();
        // A new peak dump replaces the previous one only when it is 10 % higher
        assertThat(HeapDumper.isNewPeak(new HeapDumper.HeapUsage(2200L << 20, max), 2048L << 20)).isFalse();
        assertThat(HeapDumper.isNewPeak(new HeapDumper.HeapUsage(2300L << 20, max), 2048L << 20)).isTrue();
        // Without a known maximum, only the growth counts
        assertThat(HeapDumper.isNewPeak(new HeapDumper.HeapUsage(100L << 20, 0), 0)).isTrue();
    }

    @Test
    public void cancelsTheDumpsDueLaterAndFinishesTheOneBeingWrittenAtTheEnd() throws Exception {
        ScheduledThreadPoolExecutor scheduler = HeapDumper.newScheduler();
        CountDownLatch writing = new CountDownLatch(1);
        AtomicBoolean written = new AtomicBoolean();
        AtomicBoolean later = new AtomicBoolean();
        scheduler.execute(() -> {
            writing.countDown();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                return;
            }
            written.set(true);
        });
        scheduler.schedule(() -> later.set(true), 1, TimeUnit.HOURS);
        scheduler.scheduleAtFixedRate(() -> later.set(true), 1, 1, TimeUnit.HOURS);
        writing.await();

        scheduler.shutdown();

        // The end doesn't wait an hour for the dumps that are due later
        assertThat(scheduler.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(written).isTrue();
        assertThat(later).isFalse();
    }

    @Test
    public void compressesTheDumpsWithJcmdAtTheGzipLevel() {
        assertThat(HeapDumper.dumpCommand("42", "/heap-dumps/broker-0-end.hprof", 0))
                .containsExactly("jcmd", "42", "GC.heap_dump", "/heap-dumps/broker-0-end.hprof");
        assertThat(HeapDumper.dumpCommand("42", "/heap-dumps/broker-0-end.hprof.gz", 6))
                .containsExactly("jcmd", "42", "GC.heap_dump", "-gz=6", "/heap-dumps/broker-0-end.hprof.gz");
    }

    @Test
    public void findsCompressedAndUncompressedDumps() {
        assertThat(HeapDumper.isDump("broker-0-peak.hprof")).isTrue();
        assertThat(HeapDumper.isDump("java_pid1.hprof.gz")).isTrue();
        assertThat(HeapDumper.isDump("heap-dumps.csv")).isFalse();
    }

    @Test
    public void preparesADirectoryThatEveryUserCanWriteForEachComponent() throws Exception {
        Path run = Files.createTempDirectory("heap-dumps-test");
        try {
            Map<String, Path> directories = HeapDumper.prepare(run);

            assertThat(directories).containsOnlyKeys("broker", "gateways", "applications");
            assertThat(directories.get("broker")).isEqualTo(run.resolve("heap-dumps/broker"));
            assertThat(Files.getPosixFilePermissions(directories.get("broker")))
                    .isEqualTo(PosixFilePermissions.fromString("rwxrwxrwx"));
        } finally {
            for (Path directory : HeapDumper.prepare(run).values()) {
                Files.delete(directory);
            }
            Files.delete(run.resolve("heap-dumps"));
            Files.delete(run);
        }
    }
}
