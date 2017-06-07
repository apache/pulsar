package org.apache.bookkeeper.mledger.impl;

import java.util.List;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 *  NonDurable-Cursor that doesn't read persisted message and dispatch published messages immediately.   
 *
 */
public class NonPersistentNonDurableCursorImpl extends NonDurableCursorImpl {

    private EntryAvailableCallback callback;

    NonPersistentNonDurableCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger,
            String cursorName, EntryAvailableCallback callback) {
        super(bookkeeper, config, ledger, cursorName, PositionImpl.earliest);
        this.callback = callback;
        ledger.addNonPersistentNonDurableCursors(this);
    }

    public void close() {
        ledger.removeNonPersistentNonDurableCursors(this);
    }

    public void notify(Entry entry) {
        if (callback != null) {
            callback.process(Lists.newArrayList(entry));
        }
    }

    public static interface EntryAvailableCallback {
        public void process(List<Entry> entries);
    }

    @Override
    public boolean isDurable() {
        return false;
    }

    /// Overridden methods from ManagedCursorImpl. Void implementation to skip cursor persistence

    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx) {
        /// No-Op
    }
    
    
    @Override
    void recover(final VoidCallback callback) {
        /// No-Op
    }

    @Override
    protected void internalAsyncMarkDelete(final PositionImpl newPosition, final MarkDeleteCallback callback,
            final Object ctx) {
        // Bypass persistence of mark-delete position and individually deleted messages info
        callback.markDeleteComplete(ctx);
    }

    @Override
    public void setActive() {
        /// No-Op
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public void setInactive() {
        /// No-Op
    }

    @Override
    public void asyncClose(CloseCallback callback, Object ctx) {
        // No-Op
        ledger.removeNonPersistentNonDurableCursors(this);
        callback.closeComplete(ctx);
    }

    public void asyncDeleteCursor(final String consumerName, final DeleteCursorCallback callback, final Object ctx) {
        /// No-Op
        ledger.removeNonPersistentNonDurableCursors(this);
        callback.deleteCursorComplete(ctx);
    }

    @Override
    public synchronized String toString() {
        return Objects.toStringHelper(this).add("ledger", ledger.getName()).add("cursorName", this.getName()).toString();
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentNonDurableCursorImpl.class);
    
}
