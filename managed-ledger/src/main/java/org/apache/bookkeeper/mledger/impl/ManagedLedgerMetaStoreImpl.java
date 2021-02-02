package org.apache.bookkeeper.mledger.impl;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.bookkeeper.mledger.ManagedLedgerMetaStore;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;

public class ManagedLedgerMetaStoreImpl implements ManagedLedgerMetaStore {
    final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
    final MetaStore metaStore;

    public ManagedLedgerMetaStoreImpl(MetaStore metaStore) {
        this.metaStore = metaStore;
    }
}
