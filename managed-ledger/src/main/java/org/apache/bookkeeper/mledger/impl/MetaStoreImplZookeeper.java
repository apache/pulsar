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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import com.google.common.base.Charsets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.BadVersionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:javadoctype")
public class MetaStoreImplZookeeper implements MetaStore {

    private static final Charset Encoding = Charsets.UTF_8;
    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private static final String prefixName = "/managed-ledgers";
    private static final String prefix = prefixName + "/";

    private final ZooKeeper zk;
    private final OrderedExecutor executor;

    private static class ZKStat implements Stat {
        private final int version;
        private final long creationTimestamp;
        private final long modificationTimestamp;

        ZKStat(org.apache.zookeeper.data.Stat stat) {
            this.version = stat.getVersion();
            this.creationTimestamp = stat.getCtime();
            this.modificationTimestamp = stat.getMtime();
        }

        ZKStat() {
            this.version = 0;
            this.creationTimestamp = System.currentTimeMillis();
            this.modificationTimestamp = System.currentTimeMillis();
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public long getCreationTimestamp() {
            return creationTimestamp;
        }

        @Override
        public long getModificationTimestamp() {
            return modificationTimestamp;
        }
    }

    public MetaStoreImplZookeeper(ZooKeeper zk, OrderedExecutor executor)
            throws Exception {
        this.zk = zk;
        this.executor = executor;
    }

    //
    // update timestamp if missing or 0
    // 3 cases - timestamp does not exist for ledgers serialized before
    // - timestamp is 0 for a ledger in recovery
    // - ledger has timestamp which is the normal case now

    private ManagedLedgerInfo updateMLInfoTimestamp(ManagedLedgerInfo info) {
        List<ManagedLedgerInfo.LedgerInfo> infoList = new ArrayList<>(info.getLedgerInfoCount());
        long currentTime = System.currentTimeMillis();

        for (ManagedLedgerInfo.LedgerInfo ledgerInfo : info.getLedgerInfoList()) {
            if (!ledgerInfo.hasTimestamp() || ledgerInfo.getTimestamp() == 0) {
                ManagedLedgerInfo.LedgerInfo.Builder singleInfoBuilder = ledgerInfo.toBuilder();
                singleInfoBuilder.setTimestamp(currentTime);
                infoList.add(singleInfoBuilder.build());
            } else {
                infoList.add(ledgerInfo);
            }
        }
        ManagedLedgerInfo.Builder mlInfo = ManagedLedgerInfo.newBuilder();
        mlInfo.addAllLedgerInfo(infoList);
        if (info.hasTerminatedPosition()) {
            mlInfo.setTerminatedPosition(info.getTerminatedPosition());
        }
        return mlInfo.build();
    }

    @Override
    public void getManagedLedgerInfo(final String ledgerName, boolean createIfMissing,
            final MetaStoreCallback<ManagedLedgerInfo> callback) {
        // Try to get the content or create an empty node
        zk.getData(prefix + ledgerName, false,
                (rc, path, ctx, readData, stat) -> executor.executeOrdered(ledgerName, safeRun(() -> {
                    if (rc == Code.OK.intValue()) {
                        try {
                            ManagedLedgerInfo info = parseManagedLedgerInfo(readData);
                            info = updateMLInfoTimestamp(info);
                            callback.operationComplete(info, new ZKStat(stat));
                        } catch (ParseException | InvalidProtocolBufferException e) {
                            callback.operationFailed(new MetaStoreException(e));
                        }
                    } else if (rc == Code.NONODE.intValue()) {
                        // Z-node doesn't exist
                        if (createIfMissing) {
                            log.info("Creating '{}{}'", prefix, ledgerName);

                            StringCallback createcb = (rc1, path1, ctx1, name) -> {
                                if (rc1 == Code.OK.intValue()) {
                                    ManagedLedgerInfo info = ManagedLedgerInfo.getDefaultInstance();
                                    callback.operationComplete(info, new ZKStat());
                                } else {
                                    callback.operationFailed(
                                            new MetaStoreException(KeeperException.create(Code.get(rc1))));
                                }
                            };

                            asyncCreateFullPathOptimistic(prefixName, ledgerName, new byte[0], Acl,
                                                          CreateMode.PERSISTENT, createcb);
                        } else {
                            // Tried to open a managed ledger but it doesn't exist and we shouldn't creating it at this
                            // point

                            callback.operationFailed(new ManagedLedgerException.MetadataNotFoundException(
                                    KeeperException.create(Code.get(rc))));
                        }
                    } else {
                        // Other ZK error
                        callback.operationFailed(new MetaStoreException(KeeperException.create(Code.get(rc))));
                    }
                })), null);
    }

    @Override
    public void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Stat stat,
            final MetaStoreCallback<Void> callback) {

        ZKStat zkStat = (ZKStat) stat;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Updating metadata version={} with content={}", ledgerName, zkStat.version, mlInfo);
        }

        byte[] serializedMlInfo = mlInfo.toByteArray(); // Binary format

        zk.setData(prefix + ledgerName, serializedMlInfo, zkStat.getVersion(),
                (rc, path, zkCtx, stat1) -> executor.executeOrdered(ledgerName, safeRun(() -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] UpdateLedgersIdsCallback.processResult rc={} newVersion={}", ledgerName,
                                Code.get(rc), stat != null ? stat.getVersion() : "null");
                    }
                    MetaStoreException status = null;
                    if (rc == Code.BADVERSION.intValue()) {
                        // Content has been modified on ZK since our last read
                        status = new BadVersionException(KeeperException.create(Code.get(rc)));
                        callback.operationFailed(status);
                    } else if (rc != Code.OK.intValue()) {
                        status = new MetaStoreException(KeeperException.create(Code.get(rc)));
                        callback.operationFailed(status);
                    } else {
                        callback.operationComplete(null, new ZKStat(stat1));
                    }
                })), null);
    }

    @Override
    public void getCursors(final String ledgerName, final MetaStoreCallback<List<String>> callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Get cursors list", ledgerName);
        }
        zk.getChildren(prefix + ledgerName, false,
                (rc, path, ctx, children, stat) -> executor.executeOrdered(ledgerName, safeRun(() -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] getConsumers complete rc={} children={}", ledgerName, Code.get(rc), children);
            }
            if (rc != Code.OK.intValue()) {
                callback.operationFailed(new MetaStoreException(KeeperException.create(Code.get(rc))));
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Get childrend completed version={}", ledgerName, stat.getVersion());
            }
            callback.operationComplete(children, new ZKStat(stat));
        })), null);
    }

    @Override
    public void asyncGetCursorInfo(String ledgerName, String consumerName,
            final MetaStoreCallback<ManagedCursorInfo> callback) {
        String path = prefix + ledgerName + "/" + consumerName;
        if (log.isDebugEnabled()) {
            log.debug("Reading from {}", path);
        }

        zk.getData(path, false, (rc, path1, ctx, data, stat) -> executor.executeOrdered(ledgerName, safeRun(() -> {
            if (rc != Code.OK.intValue()) {
                callback.operationFailed(new MetaStoreException(KeeperException.create(Code.get(rc))));
            } else {
                try {
                    ManagedCursorInfo info = parseManagedCursorInfo(data);
                    callback.operationComplete(info, new ZKStat(stat));
                } catch (ParseException | InvalidProtocolBufferException e) {
                    callback.operationFailed(new MetaStoreException(e));
                }
            }
        })), null);

        if (log.isDebugEnabled()) {
            log.debug("Reading from {} ok", path);
        }
    }

    @Override
    public void asyncUpdateCursorInfo(final String ledgerName, final String cursorName, final ManagedCursorInfo info,
            Stat stat, final MetaStoreCallback<Void> callback) {
        log.info("[{}] [{}] Updating cursor info ledgerId={} mark-delete={}:{}", ledgerName, cursorName,
                info.getCursorsLedgerId(), info.getMarkDeleteLedgerId(), info.getMarkDeleteEntryId());

        String path = prefix + ledgerName + "/" + cursorName;
        byte[] content = info.toByteArray(); // Binary format

        if (stat == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Creating consumer {} on meta-data store with {}", ledgerName, cursorName, info);
            }
            zk.create(path, content, Acl, CreateMode.PERSISTENT,
                    (rc, path1, ctx, name) -> executor.executeOrdered(ledgerName, safeRun(() -> {
                        if (rc != Code.OK.intValue()) {
                            log.warn("[{}] Error creating cosumer {} node on meta-data store with {}: ", ledgerName,
                                    cursorName, info, Code.get(rc));
                            callback.operationFailed(new MetaStoreException(KeeperException.create(Code.get(rc))));
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Created consumer {} on meta-data store with {}", ledgerName, cursorName,
                                        info);
                            }
                            callback.operationComplete(null, new ZKStat());
                        }
                    })), null);
        } else {
            ZKStat zkStat = (ZKStat) stat;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Updating consumer {} on meta-data store with {}", ledgerName, cursorName, info);
            }
            zk.setData(path, content, zkStat.getVersion(),
                    (rc, path1, ctx, stat1) -> executor.executeOrdered(ledgerName, safeRun(() -> {
                if (rc == Code.BADVERSION.intValue()) {
                    callback.operationFailed(new BadVersionException(KeeperException.create(Code.get(rc))));
                } else if (rc != Code.OK.intValue()) {
                    callback.operationFailed(new MetaStoreException(KeeperException.create(Code.get(rc))));
                } else {
                    callback.operationComplete(null, new ZKStat(stat1));
                }
            })), null);
        }
    }

    @Override
    public void asyncRemoveCursor(final String ledgerName, final String consumerName,
            final MetaStoreCallback<Void> callback) {
        log.info("[{}] Remove consumer={}", ledgerName, consumerName);
        zk.delete(prefix + ledgerName + "/" + consumerName, -1,
                (rc, path, ctx) -> executor.executeOrdered(ledgerName, safeRun(() -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] zk delete done. rc={}", ledgerName, consumerName, Code.get(rc));
            }
            if (rc == Code.OK.intValue()) {
                callback.operationComplete(null, null);
            } else {
                callback.operationFailed(new MetaStoreException(KeeperException.create(Code.get(rc))));
            }
        })), null);
    }

    @Override
    public void removeManagedLedger(String ledgerName, MetaStoreCallback<Void> callback) {
        log.info("[{}] Remove ManagedLedger", ledgerName);
        zk.delete(prefix + ledgerName, -1, (rc, path, ctx) -> executor.executeOrdered(ledgerName, safeRun(() -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] zk delete done. rc={}", ledgerName, Code.get(rc));
            }
            if (rc == Code.OK.intValue()) {
                callback.operationComplete(null, null);
            } else {
                callback.operationFailed(new MetaStoreException(KeeperException.create(Code.get(rc))));
            }
        })), null);
    }

    @Override
    public Iterable<String> getManagedLedgers() throws MetaStoreException {
        try {
            return zk.getChildren(prefixName, false);
        } catch (Exception e) {
            throw new MetaStoreException(e);
        }
    }

    private ManagedLedgerInfo parseManagedLedgerInfo(byte[] data)
            throws ParseException, InvalidProtocolBufferException {
        // First try binary format, then fallback to text
        try {
            return ManagedLedgerInfo.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            // Fallback to parsing protobuf text format
            ManagedLedgerInfo.Builder builder = ManagedLedgerInfo.newBuilder();
            TextFormat.merge(new String(data, Encoding), builder);
            return builder.build();
        }
    }

    private ManagedCursorInfo parseManagedCursorInfo(byte[] data)
            throws ParseException, InvalidProtocolBufferException {
        // First try binary format, then fallback to text
        try {
            return ManagedCursorInfo.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            // Fallback to parsing protobuf text format
            ManagedCursorInfo.Builder builder = ManagedCursorInfo.newBuilder();
            TextFormat.merge(new String(data, Encoding), builder);
            return builder.build();
        }

    }

    void asyncCreateFullPathOptimistic(
            final String basePath, final String nodePath, final byte[] data,
            final List<ACL> acl, final CreateMode createMode, final StringCallback callback) {
        String fullPath = basePath + "/" + nodePath;

        zk.create(fullPath, data, acl, createMode,
                  (rc, path, ignoreCtx1, name) -> {
                      Runnable retry = () -> {
                          asyncCreateFullPathOptimistic(basePath, nodePath, data,
                                                        acl, createMode, callback);
                      };

                      Consumer<Integer> complete = (finalrc) -> {
                          executor.executeOrdered(nodePath, safeRun(() -> {
                              callback.processResult(finalrc, path, null, name);
                          }));
                      };

                      if (rc != Code.NONODE.intValue()) {
                          complete.accept(rc);
                          return;
                      }

                      // Since I got a nonode, it means that my parents don't exist
                      // create mode is persistent since ephemeral nodes can't be
                      // parents
                      String nodeParent = new File(nodePath).getParent();
                      if (nodeParent == null) {
                          zk.exists(basePath, false,
                                    (existsRc, existsPath, ignoreCtx2, stat) -> {
                                        if (existsRc == Code.OK.intValue()) {
                                            if (stat != null) {
                                                retry.run();
                                            } else {
                                                complete.accept(Code.NONODE.intValue());
                                            }
                                        } else {
                                            complete.accept(existsRc);
                                        }
                                    }, null);
                      } else {
                          nodeParent = nodeParent.replace("\\", "/");
                          asyncCreateFullPathOptimistic(
                                  basePath, nodeParent, new byte[0], acl, CreateMode.PERSISTENT,
                                  (parentRc, parentPath, ignoreCtx3, parentName) -> {
                                      if (parentRc == Code.OK.intValue() || parentRc == Code.NODEEXISTS.intValue()) {
                                          retry.run();
                                      } else {
                                          complete.accept(parentRc);
                                      }
                                  });
                      }
                  }, null);
    }

    private static final Logger log = LoggerFactory.getLogger(MetaStoreImplZookeeper.class);
}
