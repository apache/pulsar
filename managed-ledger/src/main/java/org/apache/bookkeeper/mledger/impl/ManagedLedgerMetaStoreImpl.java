package org.apache.bookkeeper.mledger.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMetaStore;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.metadata.api.Stat;

public class ManagedLedgerMetaStoreImpl implements ManagedLedgerMetaStore {
    final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
    final MetaStore store;

    public ManagedLedgerMetaStoreImpl(MetaStore metaStore) {
        this.store = metaStore;
    }

    static public class InitializeResult {
        private Stat stat;
        private Optional<PositionImpl> terminatedPosition;
        private Map<String, String> propertiesMap;

        public InitializeResult(Stat stat,
                                Optional<PositionImpl> terminatedPosition,
                                Map<String, String> propertiesMap) {
            this.stat = stat;
            this.terminatedPosition = terminatedPosition;
            this.propertiesMap = propertiesMap;
        }

        public Stat getStat() {
            return stat;
        }

        public Optional<PositionImpl> getTerminatedPosition() {
            return terminatedPosition;
        }

        public Map<String, String> getPropertiesMap() {
            return propertiesMap;
        }
    }

    synchronized CompletableFuture<InitializeResult> initialize(String name, boolean createIfMissing) {
        final CompletableFuture<InitializeResult> initResult = new CompletableFuture<>();
        store.getManagedLedgerInfo(name, createIfMissing,
                new MetaStore.MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                    @Override
                    public void operationComplete(MLDataFormats.ManagedLedgerInfo result, Stat stat) {
                        final Optional<PositionImpl> terminatedPosition;
                        if (result.hasTerminatedPosition()) {
                            terminatedPosition = Optional.of(new PositionImpl(result.getTerminatedPosition()));
                        } else {
                            terminatedPosition = Optional.empty();
                        }
                        final Map<String, String> propertiesMap = new HashMap<>();

                        if (result.getPropertiesCount() > 0) {
                            for (int i = 0; i < result.getPropertiesCount(); i++) {
                                MLDataFormats.KeyValue property = result.getProperties(i);
                                propertiesMap.put(property.getKey(), property.getValue());
                            }
                        }


                        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : result.getLedgerInfoList()) {
                            ledgers.put(ls.getLedgerId(), ls);
                        }

                        initResult.complete(new InitializeResult(stat, terminatedPosition, propertiesMap));
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                        initResult.completeExceptionally(e);
                    }
                });
        return initResult;
    }
}
