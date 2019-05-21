package org.apache.bookkeeper.mledger.offload.filesystem.impl;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.filesystem.FileSystemEntryBytesReader;
import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexFileBuilder;
import org.apache.bookkeeper.mledger.offload.filesystem.TieredStorageConfigurationData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public class FileSystemManagedLedgerOffloader implements LedgerOffloader {

    private static final Logger log = LoggerFactory.getLogger(FileSystemManagedLedgerOffloader.class);

    private static final String[] DRIVER_NAMES = {"hdfs"};
    private final String driverName;
    private final String userName;
    private final String storageBasePath;
    private FileSystem fileSystem;
    private OrderedScheduler scheduler;
    private Map<String, String> configMap = new HashMap<>();
    public static boolean driverSupported(String driver) {
        return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(driver));
    }
    @Override
    public String getOffloadDriverName() {
        return driverName;
    }

    public static FileSystemManagedLedgerOffloader create(TieredStorageConfigurationData data, Map<String, String> userMetadata, OrderedScheduler scheduler) throws IOException {
        return new FileSystemManagedLedgerOffloader(data.getManagedLedgerOffloadDriver(),
                data.getHdfsFileSystemManagedLedgerOffloadUserName(),
                data.getHdfsFileSystemManagedLedgerOffloadStorageBasePath(),
                data.getHdfsFileSystemManagedLedgerOffloadAccessUri(),
                scheduler);
    }

    private FileSystemManagedLedgerOffloader(String diverName, String userName, String storageBasePath, String accessUri, OrderedScheduler scheduler) throws IOException{
        this.driverName = diverName;
        this.userName = userName;
        this.storageBasePath = storageBasePath;
        this.scheduler = scheduler;
        Configuration configuration = new Configuration();
        try {
            this.fileSystem = this.userName == null ? FileSystem.get(new URI(accessUri), configuration) :
                    FileSystem.get(new URI(accessUri), configuration, userName);
        } catch (InterruptedException | IOException | URISyntaxException e) {
            if (e instanceof URISyntaxException) {
                throw new IOException("File system's uri is wrong");
            } else if (e instanceof InterruptedException) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return null;
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle, UUID uid, Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).submit(() -> {
            if (readHandle.getLength() == 0 || !readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
                promise.completeExceptionally(
                        new IllegalArgumentException("An empty or open ledger should never be offloaded"));
                return;
            }
            long ledgerId = readHandle.getId();
            String storagePath = createStoragePath(extraMetadata.get("ManagedLedgerName"));
            String indexFilePath = createIndexFilePath(storagePath, ledgerId, uid);
            String dataFilePath = createDataFilePath(storagePath, ledgerId, uid);

            OffloadIndexFileBuilder indexFileBuilder = OffloadIndexFileBuilder.create()
                    .withLedgerMetadata(readHandle.getLedgerMetadata())
                    .withIndexFilePath(indexFilePath)
                    .withDataHeaderLength(FileSystemEntryBytesReader.getDataHeaderLength());
            try {
                FSDataOutputStream dataOutputStream = fileSystem.create(new Path(dataFilePath));
                FSDataOutputStream indexOutputStream = fileSystem.create(new Path(indexFilePath));

                FileSystemEntryBytesReader reader = new FileSystemEntryBytesReaderImpl(readHandle, configMap, indexFileBuilder);
                dataOutputStream.writeInt(reader.getDataFileMagicWord());
                byte[] headerUnUse = new byte[reader.getHeaderUnUseSize()];
                for (int i = 0; i<reader.getHeaderUnUseSize(); i++) {
                    headerUnUse[i] = 0;
                }
                dataOutputStream.write(headerUnUse);
                do {
                    ByteBuf entryBuf = reader.readEntries();
                    byte[] entryBytes = new byte[entryBuf.readableBytes()];
                    entryBuf.readBytes(entryBytes);
                    dataOutputStream.write(entryBytes);
                } while (reader.whetherCanContinueRead());
                dataOutputStream.close();
                indexFileBuilder.withDataHeaderLength(reader.getHeaderSize())
                        .withDataObjectLength(reader.getDataObjectLength());
                indexFileBuilder.build().writeIndexDataIntoFile(indexOutputStream);
                promise.complete(null);
            } catch (IOException e) {
                e.printStackTrace();
                promise.completeExceptionally(e);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid, Map<String, String> offloadDriverMetadata) {

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String storagePath = createStoragePath(offloadDriverMetadata.get("ManagedLedgerName"));
        String indexFilePath = createIndexFilePath(storagePath, ledgerId, uuid);
        String dataFilePath = createDataFilePath(storagePath, ledgerId, uuid);
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                promise.complete(FileStoreBackedReadHandleImpl.open(scheduler.chooseThread(ledgerId),
                        dataFilePath,
                        indexFilePath, fileSystem, ledgerId));
            } catch (Throwable t) {
                log.error("Failed readOffloaded: ", t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    private String createStoragePath(String managedLedgerName) {
        return storageBasePath + "/" + managedLedgerName + "/";
    }

    private String createIndexFilePath(String storagePath, long ledgerId, UUID uuid) {
        return storagePath + ledgerId + "-" + uuid + ".index";
    }

    private String createDataFilePath(String storagePath, long ledgerId, UUID uuid) {
        return storagePath + ledgerId + "-" + uuid + ".log";
    }
    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
        String storagePath = createStoragePath(offloadDriverMetadata.get("ManagedLedgerName"));
        String indexFilePath = createIndexFilePath(storagePath, ledgerId, uid);
        String dataFilePath = createDataFilePath(storagePath, ledgerId, uid);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        try {

            fileSystem.delete(new Path(indexFilePath), false);
            fileSystem.delete(new Path(dataFilePath), false);
            promise.complete(null);
        } catch (IOException e) {
            log.error("Failed to delete Offloaded: ", e);
            promise.completeExceptionally(e);
        }
        return promise;
    }
}
