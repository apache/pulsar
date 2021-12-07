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

package org.apache.pulsar.metadata.impl;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

/**
 *
 */
@Slf4j
public class RocksdbMetadataStore extends AbstractMetadataStore {
    private static final byte[] SEQUENTIAL_ID_KEY = toBytes("__metadata_sequentialId_key");
    private static final byte[] INSTANCE_ID_KEY = toBytes("__metadata_instanceId_key");

    private final long instanceId;
    private final AtomicLong sequentialIdGenerator;

    private final TransactionDB db;
    private final ReentrantReadWriteLock dbStateLock;
    private volatile State state;

    private final WriteOptions optionSync;
    private final WriteOptions optionDontSync;
    private final ReadOptions optionCache;
    private final ReadOptions optionDontCache;

    enum State {
        RUNNING, CLOSED
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class MetaValue {
        private static final int HEADER_SIZE = 4 + 4 + 8 + 8 + 8 + 8 + 1;
        private static final int FORMAT_VERSION_V1 = 1;

        long version;
        long owner;
        long createdTimestamp;
        long modifiedTimestamp;
        boolean ephemeral;
        byte[] data;

        /**
         * Note: we can only add new fields, but not change or remove existing fields.
         */
        public byte[] serialize() {
            byte[] result = new byte[HEADER_SIZE + data.length];
            ByteBuffer buffer = ByteBuffer.wrap(result);
            buffer.putInt(HEADER_SIZE);
            buffer.putInt(FORMAT_VERSION_V1);
            buffer.putLong(version);
            buffer.putLong(owner);
            buffer.putLong(createdTimestamp);
            buffer.putLong(modifiedTimestamp);
            buffer.put((byte) (ephemeral ? 1 : 0));
            buffer.put(data);
            return result;
        }

        public static MetaValue parse(byte[] dataBytes) throws MetadataStoreException {
            if (dataBytes == null) {
                return null;
            }
            if (dataBytes.length < 4) {
                throw new MetadataStoreException("Invalid MetaValue data, size=" + dataBytes.length);
            }
            ByteBuffer buffer = ByteBuffer.wrap(dataBytes);
            MetaValue metaValue = new MetaValue();
            int headerSize = buffer.getInt();
            if (dataBytes.length < headerSize) {
                throw new MetadataStoreException(
                        String.format("Invalid MetaValue data, no enough header data. expect %d, actual %d",
                                headerSize, dataBytes.length));
            }
            int formatVersion = buffer.getInt();
            if (formatVersion >= FORMAT_VERSION_V1) {
                metaValue.version = buffer.getLong();
                metaValue.owner = buffer.getLong();
                metaValue.createdTimestamp = buffer.getLong();
                metaValue.modifiedTimestamp = buffer.getLong();
                metaValue.ephemeral = buffer.get() > 0;
            } else {
                throw new MetadataStoreException("Invalid MetaValue format version=" + formatVersion);
            }
            buffer.position(headerSize);
            metaValue.data = new byte[buffer.remaining()];
            buffer.get(metaValue.data);
            return metaValue;
        }
    }

    @VisibleForTesting
    static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @VisibleForTesting
    static String toString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @VisibleForTesting
    static byte[] toBytes(long value) {
        return ByteBuffer.wrap(new byte[8]).putLong(value).array();
    }

    @VisibleForTesting
    static long toLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * @param metadataURL         format "rocksdb://{storePath}"
     * @param metadataStoreConfig
     * @throws MetadataStoreException
     */
    public RocksdbMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig)
            throws MetadataStoreException {
        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            throw new MetadataStoreException("Failed to load RocksDB JNI library", t);
        }

        String dataDir = metadataURL.substring("rocksdb://".length());
        Path dataPath = FileSystems.getDefault().getPath(dataDir);
        try {
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        db = openDB(dataPath.toString(), metadataStoreConfig.getConfigFilePath());

        this.optionSync = new WriteOptions().setSync(true);
        this.optionDontSync = new WriteOptions().setSync(false);
        this.optionCache = new ReadOptions().setFillCache(true);
        this.optionDontCache = new ReadOptions().setFillCache(false);

        try {
            sequentialIdGenerator = loadSequentialIdGenerator();
            instanceId = loadInstanceId();
        } catch (RocksDBException exception) {
            log.error("Error while init metastore state", exception);
            close();
            throw new MetadataStoreException("Error init metastore state", exception);
        }
        state = State.RUNNING;
        dbStateLock = new ReentrantReadWriteLock();
        log.info("new RocksdbMetadataStore,url={},instanceId={}", metadataStoreConfig, instanceId);
    }

    private long loadInstanceId() throws RocksDBException {
        long instanceId;
        byte[] value = db.get(optionDontCache, INSTANCE_ID_KEY);
        if (value != null) {
            instanceId = toLong(value) + 1;
        } else {
            instanceId = 0;
        }
        db.put(optionSync, INSTANCE_ID_KEY, toBytes(instanceId));
        return instanceId;
    }

    private AtomicLong loadSequentialIdGenerator() throws RocksDBException {
        AtomicLong generator = new AtomicLong(0);
        byte[] value = db.get(optionDontCache, SEQUENTIAL_ID_KEY);
        if (value != null) {
            generator.set(toLong(value));
        } else {
            db.put(optionSync, INSTANCE_ID_KEY, toBytes(generator.get()));
        }
        return generator;
    }

    private TransactionDB openDB(String dataPath, String configFilePath) throws MetadataStoreException {
        try (TransactionDBOptions transactionDBOptions = new TransactionDBOptions()) {
            if (configFilePath != null) {
                DBOptions dbOptions = new DBOptions();
                ConfigOptions configOptions = new ConfigOptions();
                List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
                OptionsUtil.loadOptionsFromFile(configOptions, configFilePath, dbOptions, cfDescriptors);
                log.info("Load options from configFile({}), CF.size={},dbConfig={}", configFilePath,
                        cfDescriptors.size(), dbOptions);
                if (log.isDebugEnabled()) {
                    for (ColumnFamilyDescriptor cfDescriptor : cfDescriptors) {
                        log.debug("CF={},Options={}", cfDescriptor.getName(), cfDescriptor.getOptions().toString());
                    }
                }
                List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
                try {
                    return TransactionDB.open(dbOptions, transactionDBOptions, dataPath, cfDescriptors, cfHandles);
                } finally {
                    dbOptions.close();
                    configOptions.close();
                }
            } else {
                Options options = new Options();
                options.setCreateIfMissing(true);
                configLog(options);
                try {
                    return TransactionDB.open(options, transactionDBOptions, dataPath);
                } finally {
                    options.close();
                }
            }
        } catch (RocksDBException e) {
            throw new MetadataStoreException("Error open RocksDB database", e);
        } catch (Throwable t) {
            throw MetadataStoreException.wrap(t);
        }
    }

    private void configLog(Options options) throws IOException {
        // Configure file path
        String logPath = System.getProperty("pulsar.log.dir", "");
        Path logPathSetting;
        if (!logPath.isEmpty()) {
            logPathSetting = FileSystems.getDefault().getPath(logPath + "/rocksdb-log");
            Files.createDirectories(logPathSetting);
            options.setDbLogDir(logPathSetting.toString());
        }

        // Configure log level
        String logLevel = System.getProperty("pulsar.log.level", "info");
        switch (logLevel) {
            case "debug":
                options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
                break;
            case "info":
                options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
                break;
            case "warn":
                options.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
                break;
            case "error":
                options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
                break;
            default:
                log.warn("Unrecognized RockDB log level: {}", logLevel);
        }

        // Keep log files for 1month
        options.setKeepLogFileNum(30);
        options.setLogFileTimeToRoll(TimeUnit.DAYS.toSeconds(1));
    }

    @Override
    public void close() throws MetadataStoreException {
        if (state == State.CLOSED) {
            //already closed.
            return;
        }
        try {
            dbStateLock.writeLock().lock();
            state = State.CLOSED;
            log.info("close.instanceId={}", instanceId);
            db.close();
            optionSync.close();
            optionDontSync.close();
            optionCache.close();
            optionDontCache.close();
            super.close();
        } catch (Throwable throwable) {
            throw MetadataStoreException.wrap(throwable);
        } finally {
            dbStateLock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Optional<GetResult>> storeGet(String path) {
        if (log.isDebugEnabled()) {
            log.debug("getFromStore.path={},instanceId={}", path, instanceId);
        }
        try {
            dbStateLock.readLock().lock();
            if (state == State.CLOSED) {
                throw new MetadataStoreException.AlreadyClosedException("");
            }
            byte[] value = db.get(optionCache, toBytes(path));
            if (value == null) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            MetaValue metaValue = MetaValue.parse(value);
            if (metaValue.ephemeral && metaValue.owner != instanceId) {
                delete(path, Optional.empty());
                return CompletableFuture.completedFuture(Optional.empty());
            }

            GetResult result = new GetResult(metaValue.getData(),
                    new Stat(path,
                            metaValue.getVersion(),
                            metaValue.getCreatedTimestamp(),
                            metaValue.getModifiedTimestamp(),
                            metaValue.ephemeral,
                            metaValue.getOwner() == instanceId));
            return CompletableFuture.completedFuture(Optional.of(result));
        } catch (Throwable e) {
            return FutureUtil.failedFuture(MetadataStoreException.wrap(e));
        } finally {
            dbStateLock.readLock().unlock();
        }
    }

    @Override
    protected CompletableFuture<List<String>> getChildrenFromStore(String path) {
        if (log.isDebugEnabled()) {
            log.debug("getChildrenFromStore.path={},instanceId={}", path, instanceId);
        }
        try {
            dbStateLock.readLock().lock();
            if (state == State.CLOSED) {
                throw new MetadataStoreException.AlreadyClosedException("");
            }
            try (RocksIterator iterator = db.newIterator(optionDontCache)) {
                Set<String> result = new HashSet<>();
                String firstKey = path.equals("/") ? path : path + "/";
                String lastKey = path.equals("/") ? "0" : path + "0"; // '0' is lexicographically just after '/'
                for (iterator.seek(toBytes(firstKey)); iterator.isValid(); iterator.next()) {
                    String currentPath = toString(iterator.key());
                    if (lastKey.compareTo(currentPath) <= 0) {
                        //End of sub paths.
                        break;
                    }
                    byte[] value = iterator.value();
                    if (value == null) {
                        continue;
                    }
                    MetaValue metaValue = MetaValue.parse(value);
                    if (metaValue.ephemeral && metaValue.owner != instanceId) {
                        delete(currentPath, Optional.empty());
                        continue;
                    }
                    //check if it's direct child.
                    String relativePath = currentPath.substring(firstKey.length());
                    String child = relativePath.split("/", 2)[0];
                    result.add(child);
                }
                List<String> arrayResult = new ArrayList<>(result);
                arrayResult.sort(Comparator.naturalOrder());
                return CompletableFuture.completedFuture(arrayResult);
            }
        } catch (Throwable e) {
            return FutureUtil.failedFuture(MetadataStoreException.wrap(e));
        } finally {
            dbStateLock.readLock().unlock();
        }
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        if (log.isDebugEnabled()) {
            log.debug("existsFromStore.path={},instanceId={}", path, instanceId);
        }
        try {
            dbStateLock.readLock().lock();
            if (state == State.CLOSED) {
                throw new MetadataStoreException.AlreadyClosedException("");
            }
            byte[] value = db.get(optionDontCache, toBytes(path));
            if (log.isDebugEnabled()) {
                if (value != null) {
                    MetaValue metaValue = MetaValue.parse(value);
                    log.debug("Get data from db:{}.", metaValue);
                }
            }
            return CompletableFuture.completedFuture(value != null);
        } catch (Throwable e) {
            return FutureUtil.failedFuture(MetadataStoreException.wrap(e));
        } finally {
            dbStateLock.readLock().unlock();
        }
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        if (log.isDebugEnabled()) {
            log.debug("storeDelete.path={},instanceId={}", path, instanceId);
        }
        try {
            dbStateLock.readLock().lock();
            if (state == State.CLOSED) {
                throw new MetadataStoreException.AlreadyClosedException("");
            }
            try (Transaction transaction = db.beginTransaction(optionSync)) {
                byte[] pathBytes = toBytes(path);
                byte[] oldValueData = transaction.getForUpdate(optionDontCache, pathBytes, false);
                MetaValue metaValue = MetaValue.parse(oldValueData);
                if (metaValue == null) {
                    throw new MetadataStoreException.NotFoundException(String.format("path %s not found.", path));
                }
                if (expectedVersion.isPresent() && !expectedVersion.get().equals(metaValue.getVersion())) {
                    throw new MetadataStoreException.BadVersionException(
                            String.format("Version mismatch, actual=%s, expect=%s", metaValue.getVersion(),
                                    expectedVersion.get()));
                }
                transaction.delete(pathBytes);
                transaction.commit();
                receivedNotification(new Notification(NotificationType.Deleted, path));
                notifyParentChildrenChanged(path);
                return CompletableFuture.completedFuture(null);
            }
        } catch (Throwable e) {
            return FutureUtil.failedFuture(MetadataStoreException.wrap(e));
        } finally {
            dbStateLock.readLock().unlock();
        }
    }

    @Override
    protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> expectedVersion,
                                               EnumSet<CreateOption> options) {
        if (log.isDebugEnabled()) {
            log.debug("storePut.path={},instanceId={}", path, instanceId);
        }
        try {
            dbStateLock.readLock().lock();
            if (state == State.CLOSED) {
                throw new MetadataStoreException.AlreadyClosedException("");
            }
            try (Transaction transaction = db.beginTransaction(optionSync)) {
                byte[] pathBytes = toBytes(path);
                byte[] oldValueData = transaction.getForUpdate(optionDontCache, pathBytes, false);
                MetaValue metaValue = MetaValue.parse(oldValueData);
                if (expectedVersion.isPresent()) {
                    if (metaValue == null && expectedVersion.get() != -1 ||
                            metaValue != null && !expectedVersion.get().equals(metaValue.getVersion())) {
                        throw new MetadataStoreException.BadVersionException(
                                String.format("Version mismatch, actual=%s, expect=%s",
                                        metaValue == null ? null : metaValue.getVersion(), expectedVersion.get()));
                    }
                }

                boolean created = false;
                long timestamp = System.currentTimeMillis();
                if (metaValue == null) {
                    // create new node
                    metaValue = new MetaValue();
                    metaValue.version = 0;
                    metaValue.createdTimestamp = timestamp;
                    metaValue.ephemeral = options.contains(CreateOption.Ephemeral);
                    if (options.contains(CreateOption.Sequential)) {
                        path += sequentialIdGenerator.getAndIncrement();
                        pathBytes = toBytes(path);
                        transaction.put(SEQUENTIAL_ID_KEY, toBytes(sequentialIdGenerator.get()));
                    }
                    created = true;
                } else {
                    // update old node
                    metaValue.version++;
                }
                metaValue.modifiedTimestamp = timestamp;
                metaValue.owner = instanceId;
                metaValue.data = data;

                //handle Sequential
                transaction.put(pathBytes, metaValue.serialize());

                transaction.commit();

                receivedNotification(
                        new Notification(created ? NotificationType.Created : NotificationType.Modified, path));
                if (created) {
                    notifyParentChildrenChanged(path);
                }

                return CompletableFuture.completedFuture(
                        new Stat(path, metaValue.version, metaValue.createdTimestamp, metaValue.modifiedTimestamp,
                                metaValue.ephemeral, true));
            }
        } catch (Throwable e) {
            return FutureUtil.failedFuture(MetadataStoreException.wrap(e));
        } finally {
            dbStateLock.readLock().unlock();
        }
    }
}
