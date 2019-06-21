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
package org.apache.bookkeeper.mledger.offload.filesystem.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.filesystem.FileSystemLedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.offload.filesystem.FileSystemConfigurationData;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public class FileSystemManagedLedgerOffloader implements LedgerOffloader {

    private static final Logger log = LoggerFactory.getLogger(FileSystemManagedLedgerOffloader.class);
    private static final String STORAGE_BASE_PATH = "storageBasePath";
    private static final String DRIVER_NAMES = "filesystem";
    private static final String MANAGED_LEDGER_NAME = "ManagedLedgerName";
    static final long METADATA_KEY_INDEX = -1;
    private final Configuration configuration;
    private final String driverName;
    private final String storageBasePath;
    private final FileSystem fileSystem;
    private OrderedScheduler scheduler;
    private static final long ENTRIES_PER_READ = 100;
    private OrderedScheduler assignmentScheduler;
    public static boolean driverSupported(String driver) {
        return DRIVER_NAMES.equals(driver);
    }
    @Override
    public String getOffloadDriverName() {
        return driverName;
    }

    public static FileSystemManagedLedgerOffloader create(FileSystemConfigurationData conf, OrderedScheduler scheduler) throws IOException {
        return new FileSystemManagedLedgerOffloader(conf, scheduler);
    }

    private FileSystemManagedLedgerOffloader(FileSystemConfigurationData conf, OrderedScheduler scheduler) throws IOException {
        this.configuration = new Configuration();
        if (conf.getFileSystemProfilePath() != null) {
            String[] paths = conf.getFileSystemProfilePath().split(",");
            for (int i =0 ; i < paths.length; i++) {
                configuration.addResource(new Path(paths[i]));
            }
        }
        if (!"".equals(conf.getFileSystemURI()) && conf.getFileSystemURI() != null) {
            configuration.set("fs.defaultFS", conf.getFileSystemURI());
        }
        if (configuration.get("fs.hdfs.impl") == null) {
            this.configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        if (configuration.get("fs.file.impl") == null) {
            this.configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        }

        this.configuration.setClassLoader(FileSystemLedgerOffloaderFactory.class.getClassLoader());
        this.driverName = conf.getManagedLedgerOffloadDriver();
        this.storageBasePath = configuration.get("hadoop.tmp.dir");
        this.scheduler = scheduler;
        this.fileSystem = FileSystem.get(configuration);
        this.assignmentScheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(conf.getManagedLedgerOffloadMaxThreads())
                .name("offload-assignment").build();
    }
    @VisibleForTesting
    public FileSystemManagedLedgerOffloader(FileSystemConfigurationData conf, OrderedScheduler scheduler, String testHDFSPath, String baseDir) throws IOException {
        this.configuration = new Configuration();
        this.configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        this.configuration.set("fs.defaultFS", testHDFSPath);
        this.configuration.setClassLoader(FileSystemLedgerOffloaderFactory.class.getClassLoader());
        this.driverName = conf.getManagedLedgerOffloadDriver();
        this.configuration.set("hadoop.tmp.dir", baseDir);
        this.storageBasePath = baseDir;
        this.scheduler = scheduler;
        this.fileSystem = FileSystem.get(configuration);
        this.assignmentScheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(conf.getManagedLedgerOffloadMaxThreads())
                .name("offload-assignment").build();
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        String path = storageBasePath == null ? "null" : storageBasePath;
        return ImmutableMap.of(
                STORAGE_BASE_PATH, path
        );
    }

    /*
    * ledgerMetadata stored in an index of -1
    * */
    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle, UUID uuid, Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).submit(new LedgerReader(readHandle, uuid, extraMetadata, promise, storageBasePath, configuration, assignmentScheduler));
        return promise;
    }

    private static class LedgerReader implements Runnable {

        private final ReadHandle readHandle;
        private final UUID uuid;
        private final Map<String, String> extraMetadata;
        private final CompletableFuture<Void> promise;
        private final String storageBasePath;
        private final Configuration configuration;
        volatile Exception fileSystemWriteException = null;
        private OrderedScheduler assignmentScheduler;

        private LedgerReader(ReadHandle readHandle, UUID uuid, Map<String, String> extraMetadata, CompletableFuture<Void> promise,
                             String storageBasePath, Configuration configuration, OrderedScheduler assignmentScheduler) {
            this.readHandle = readHandle;
            this.uuid = uuid;
            this.extraMetadata = extraMetadata;
            this.promise = promise;
            this.storageBasePath = storageBasePath;
            this.configuration = configuration;
            this.assignmentScheduler = assignmentScheduler;
        }

        @Override
        public void run() {
            if (readHandle.getLength() == 0 || !readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
                promise.completeExceptionally(
                        new IllegalArgumentException("An empty or open ledger should never be offloaded"));
                return;
            }
            long ledgerId = readHandle.getId();
            String storagePath = getStoragePath(storageBasePath, extraMetadata.get(MANAGED_LEDGER_NAME));
            String dataFilePath = getDataFilePath(storagePath, ledgerId, uuid);
            LongWritable key = new LongWritable();
            BytesWritable value = new BytesWritable();
            try {
                MapFile.Writer dataWriter = new MapFile.Writer(configuration,
                        new Path(dataFilePath),
                        MapFile.Writer.keyClass(LongWritable.class),
                        MapFile.Writer.valueClass(BytesWritable.class));
                //store the ledgerMetadata in -1 index
                key.set(METADATA_KEY_INDEX);
                byte[] ledgerMetadata = buildLedgerMetadataFormat(readHandle.getLedgerMetadata());
                value.set(buildLedgerMetadataFormat(readHandle.getLedgerMetadata()), 0, ledgerMetadata.length);
                dataWriter.append(key, value);
                AtomicLong haveOffloadEntryNumber = new AtomicLong(0);
                long needToOffloadFirstEntryNumber = 0;
                CountDownLatch countDownLatch;
                do {
                    long end = Math.min(needToOffloadFirstEntryNumber + ENTRIES_PER_READ - 1, readHandle.getLastAddConfirmed());
                    log.debug("read ledger entries. start: {}, end: {}", needToOffloadFirstEntryNumber, end);
                    LedgerEntries ledgerEntriesOnce = readHandle.readAsync(needToOffloadFirstEntryNumber, end).get();
                    countDownLatch = new CountDownLatch(1);
                    assignmentScheduler.chooseThread(ledgerId).submit(new FileSystemWriter(ledgerEntriesOnce, dataWriter,
                            countDownLatch, haveOffloadEntryNumber, this)).addListener(() -> {}, Executors.newSingleThreadExecutor());
                    needToOffloadFirstEntryNumber = end + 1;
                } while (needToOffloadFirstEntryNumber - 1 != readHandle.getLastAddConfirmed() && fileSystemWriteException == null);
                countDownLatch.await();
                if (fileSystemWriteException != null) {
                    throw fileSystemWriteException;
                }
                IOUtils.closeStream(dataWriter);
                promise.complete(null);
            } catch (Exception e) {
                log.error("Exception when get CompletableFuture<LedgerEntries> : ManagerLedgerName: {}, " +
                        "LedgerId: {}, UUID: {} ", extraMetadata.get(MANAGED_LEDGER_NAME), ledgerId, uuid, e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                promise.completeExceptionally(e);
            }
        }
    }

    private static class FileSystemWriter implements Runnable {

        private final LedgerEntries ledgerEntriesOnce;

        private final LongWritable key = new LongWritable();
        private final BytesWritable value = new BytesWritable();

        private final MapFile.Writer dataWriter;
        private final CountDownLatch countDownLatch;
        private final AtomicLong haveOffloadEntryNumber;
        private final LedgerReader ledgerReader;


        private FileSystemWriter(LedgerEntries ledgerEntriesOnce, MapFile.Writer dataWriter,
                                 CountDownLatch countDownLatch, AtomicLong haveOffloadEntryNumber, LedgerReader ledgerReader) {
            this.ledgerEntriesOnce = ledgerEntriesOnce;
            this.dataWriter = dataWriter;
            this.countDownLatch = countDownLatch;
            this.haveOffloadEntryNumber = haveOffloadEntryNumber;
            this.ledgerReader = ledgerReader;
        }

        @Override
        public void run() {
            if (ledgerReader.fileSystemWriteException == null) {
                Iterator<LedgerEntry> iterator = ledgerEntriesOnce.iterator();
                while (iterator.hasNext()) {
                    LedgerEntry entry = iterator.next();
                    long entryId = entry.getEntryId();
                    key.set(entryId);
                    try {
                        value.set(entry.getEntryBytes(), 0, entry.getEntryBytes().length);
                        dataWriter.append(key, value);
                    } catch (IOException e) {
                        ledgerReader.fileSystemWriteException = e;
                        break;
                    }
                    haveOffloadEntryNumber.incrementAndGet();
                }
            }
            countDownLatch.countDown();
        }
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid, Map<String, String> offloadDriverMetadata) {

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String storagePath = getStoragePath(storageBasePath, offloadDriverMetadata.get(MANAGED_LEDGER_NAME));
        String dataFilePath = getDataFilePath(storagePath, ledgerId, uuid);
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                MapFile.Reader reader = new MapFile.Reader(new Path(dataFilePath),
                        configuration);
                promise.complete(FileStoreBackedReadHandleImpl.open(scheduler.chooseThread(ledgerId), reader, ledgerId));
            } catch (Throwable t) {
                log.error("Failed to open FileStoreBackedReadHandleImpl: ManagerLedgerName: {}, " +
                        "LegerId: {}, UUID: {}", offloadDriverMetadata.get(MANAGED_LEDGER_NAME), ledgerId, uuid, t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    private static String getStoragePath(String storageBasePath, String managedLedgerName) {
        return storageBasePath == null ? managedLedgerName + "/" : storageBasePath + "/" + managedLedgerName + "/";
    }

    private static String getDataFilePath(String storagePath, long ledgerId, UUID uuid) {
        return storagePath + ledgerId + "-" + uuid.toString();
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
        String storagePath = getStoragePath(storageBasePath, offloadDriverMetadata.get(MANAGED_LEDGER_NAME));
        String dataFilePath = getDataFilePath(storagePath, ledgerId, uid);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        try {
            fileSystem.delete(new Path(dataFilePath), true);
            promise.complete(null);
        } catch (IOException e) {
            log.error("Failed to delete Offloaded: ", e);
            promise.completeExceptionally(e);
        }
        return promise;
    }

    private static byte[] buildLedgerMetadataFormat(LedgerMetadata metadata) {
        DataFormats.LedgerMetadataFormat.Builder builder = DataFormats.LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(metadata.getWriteQuorumSize())
                .setAckQuorumSize(metadata.getAckQuorumSize())
                .setEnsembleSize(metadata.getEnsembleSize())
                .setLength(metadata.getLength())
                .setState(metadata.isClosed() ? DataFormats.LedgerMetadataFormat.State.CLOSED : DataFormats.LedgerMetadataFormat.State.OPEN)
                .setLastEntryId(metadata.getLastEntryId())
                .setCtime(metadata.getCtime())
                .setDigestType(BookKeeper.DigestType.toProtoDigestType(
                        BookKeeper.DigestType.fromApiDigestType(metadata.getDigestType())));

        for (Map.Entry<String, byte[]> e : metadata.getCustomMetadata().entrySet()) {
            builder.addCustomMetadataBuilder()
                    .setKey(e.getKey()).setValue(ByteString.copyFrom(e.getValue()));
        }

        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> e : metadata.getAllEnsembles().entrySet()) {
            builder.addSegmentBuilder()
                    .setFirstEntryId(e.getKey())
                    .addAllEnsembleMember(e.getValue().stream().map(a -> a.toString()).collect(Collectors.toList()));
        }

        return builder.build().toByteArray();
    }
}
