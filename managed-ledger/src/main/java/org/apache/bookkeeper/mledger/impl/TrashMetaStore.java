package org.apache.bookkeeper.mledger.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

public interface TrashMetaStore {

    String MANAGED_LEDGER = "managed-ledger";

    String MANAGED_CURSOR = "managed-cursor";

    String SCHEMA = "schema";

    interface TrashMetaStoreCallback<T> {

        void operationComplete(T result);

        void operationFailed(ManagedLedgerException.MetaStoreException e);
    }

    CompletableFuture<Void> initialize();

    CompletableFuture<Void> appendLedgerTrashData(long ledgerId, LedgerInfo context);

    CompletableFuture<Void> appendOffloadLedgerTrashData(long ledgerId, LedgerInfo context);

    void asyncUpdateTrashData(TrashMetaStore.TrashMetaStoreCallback<Void> callback);

    void triggerDelete();

    List<Long> getAllArchiveIndex();

    Map<String, LedgerInfo> getArchiveData(Long index);
}
