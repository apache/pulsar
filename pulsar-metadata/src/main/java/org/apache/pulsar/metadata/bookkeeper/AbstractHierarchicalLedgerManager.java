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
package org.apache.pulsar.metadata.bookkeeper;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.zookeeper.AsyncCallback;

@Slf4j
abstract class AbstractHierarchicalLedgerManager {
    protected final MetadataStore store;
    protected final ScheduledExecutorService scheduler;
    protected final String ledgerRootPath;

    AbstractHierarchicalLedgerManager(MetadataStore store,
                                      ScheduledExecutorService scheduler,
                                      String ledgerRootPath) {
        this.store = store;
        this.scheduler = scheduler;
        this.ledgerRootPath = ledgerRootPath;
    }

    /**
     * regex expression for name of top level parent znode for ledgers (in
     * HierarchicalLedgerManager) or znode of a ledger (in FlatLedgerManager).
     *
     * @return
     */
    protected abstract String getLedgerParentNodeRegex();

    /**
     * whether the child of ledgersRootPath is a top level parent znode for
     * ledgers (in HierarchicalLedgerManager) or znode of a ledger (in
     * FlatLedgerManager).
     */
    public boolean isLedgerParentNode(String path) {
        return path.matches(getLedgerParentNodeRegex());
    }

    /**
     * Process hash nodes in a given path.
     */
    void asyncProcessLevelNodes(
            final String path, final BookkeeperInternalCallbacks.Processor<String> processor,
            final AsyncCallback.VoidCallback finalCb, final Object context,
            final int successRc, final int failureRc) {

        store.getChildren(path)
                .thenAccept(levelNodes -> {
                    if (levelNodes.isEmpty()) {
                        finalCb.processResult(successRc, null, context);
                        return;
                    }

                    AsyncListProcessor<String> listProcessor = new AsyncListProcessor<>(scheduler);
                    // process its children
                    listProcessor.process(levelNodes, processor, finalCb, context, successRc, failureRc);
                }).exceptionally(ex -> {
                    log.error("Error polling hash nodes of {}: {}", path, ex.getMessage());
                    finalCb.processResult(failureRc, null, context);
                    return null;
                });
    }

    /**
     * Process list one by one in asynchronize way. Process will be stopped immediately
     * when error occurred.
     */
    private static class AsyncListProcessor<T> {
        // use this to prevent long stack chains from building up in callbacks
        ScheduledExecutorService scheduler;

        /**
         * Constructor.
         *
         * @param scheduler
         *          Executor used to prevent long stack chains
         */
        public AsyncListProcessor(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
        }

        /**
         * Process list of items.
         *
         * @param data
         *          List of data to process
         * @param processor
         *          Callback to process element of list when success
         * @param finalCb
         *          Final callback to be called after all elements in the list are processed
         * @param context
         *          Context of final callback
         * @param successRc
         *          RC passed to final callback on success
         * @param failureRc
         *          RC passed to final callback on failure
         */
        public void process(final List<T> data, final BookkeeperInternalCallbacks.Processor<T> processor,
                            final AsyncCallback.VoidCallback finalCb, final Object context,
                            final int successRc, final int failureRc) {
            if (data == null || data.size() == 0) {
                finalCb.processResult(successRc, null, context);
                return;
            }
            final int size = data.size();
            final AtomicInteger current = new AtomicInteger(0);
            T firstElement = data.get(0);

            processor.process(firstElement, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (rc != successRc) {
                        // terminal immediately
                        finalCb.processResult(failureRc, null, context);
                        return;
                    }
                    // process next element
                    int next = current.incrementAndGet();
                    if (next >= size) { // reach the end of list
                        finalCb.processResult(successRc, null, context);
                        return;
                    }
                    final T dataToProcess = data.get(next);
                    final AsyncCallback.VoidCallback stub = this;
                    scheduler.submit(() -> processor.process(dataToProcess, stub));
                }
            });
        }

    }

    // get ledger from all level nodes
    long getLedgerId(String... levelNodes) throws IOException {
        return StringUtils.stringToHierarchicalLedgerId(levelNodes);
    }

    /**
     * Process ledgers in a single zk node.
     *
     * <p>
     * for each ledger found in this zk node, processor#process(ledgerId) will be triggerred
     * to process a specific ledger. after all ledgers has been processed, the finalCb will
     * be called with provided context object. The RC passed to finalCb is decided by :
     * <ul>
     * <li> All ledgers are processed successfully, successRc will be passed.
     * <li> Either ledger is processed failed, failureRc will be passed.
     * </ul>
     * </p>
     *
     * @param path
     *          Zk node path to store ledgers
     * @param processor
     *          Processor provided to process ledger
     * @param finalCb
     *          Callback object when all ledgers are processed
     * @param ctx
     *          Context object passed to finalCb
     * @param successRc
     *          RC passed to finalCb when all ledgers are processed successfully
     * @param failureRc
     *          RC passed to finalCb when either ledger is processed failed
     */
    protected void asyncProcessLedgersInSingleNode(
            final String path, final BookkeeperInternalCallbacks.Processor<Long> processor,
            final AsyncCallback.VoidCallback finalCb, final Object ctx,
            final int successRc, final int failureRc) {
        store.getChildren(path)
                .thenAccept(ledgerNodes -> {
                    Set<Long> activeLedgers = HierarchicalLedgerUtils.ledgerListToSet(ledgerNodes, ledgerRootPath, path);
                    if (log.isDebugEnabled()) {
                        log.debug("Processing ledgers: {}", activeLedgers);
                    }

                    // no ledgers found, return directly
                    if (activeLedgers.isEmpty()) {
                        finalCb.processResult(successRc, null, ctx);
                        return;
                    }

                    BookkeeperInternalCallbacks.MultiCallback
                            mcb = new BookkeeperInternalCallbacks.MultiCallback(activeLedgers.size(), finalCb, ctx,
                            successRc, failureRc);
                    // start loop over all ledgers
                    for (Long ledger : activeLedgers) {
                        processor.process(ledger, mcb);
                    }

                }).exceptionally(ex -> {
                    finalCb.processResult(failureRc, null, ctx);
                    return null;
                });
    }

}
