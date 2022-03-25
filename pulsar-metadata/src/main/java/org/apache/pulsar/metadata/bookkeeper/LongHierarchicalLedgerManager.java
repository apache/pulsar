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
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.zookeeper.AsyncCallback;

@Slf4j
class LongHierarchicalLedgerManager extends AbstractHierarchicalLedgerManager {
    public LongHierarchicalLedgerManager(MetadataStore store, ScheduledExecutorService scheduler,
                                         String ledgerRootPath) {
        super(store, scheduler, ledgerRootPath);
    }

    public long getLedgerId(String pathName) throws IOException {
        if (!pathName.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + pathName);
        }
        String hierarchicalPath = pathName.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToLongHierarchicalLedgerId(hierarchicalPath);
    }

    public String getLedgerPath(long ledgerId) {
        return ledgerRootPath + StringUtils.getLongHierarchicalLedgerPath(ledgerId);
    }

    //
    // Active Ledger Manager
    //

    public void asyncProcessLedgers(final BookkeeperInternalCallbacks.Processor<Long> processor,
                                    final AsyncCallback.VoidCallback finalCb,
                                    final Object context, final int successRc, final int failureRc) {

        // If it succeeds, proceed with our own recursive ledger processing for the 63-bit id ledgers
        asyncProcessLevelNodes(ledgerRootPath,
                new RecursiveProcessor(0, ledgerRootPath, processor, context, successRc, failureRc), finalCb, context,
                successRc, failureRc);
    }

    private class RecursiveProcessor implements BookkeeperInternalCallbacks.Processor<String> {
        private final int level;
        private final String path;
        private final BookkeeperInternalCallbacks.Processor<Long> processor;
        private final Object context;
        private final int successRc;
        private final int failureRc;

        private RecursiveProcessor(int level, String path, BookkeeperInternalCallbacks.Processor<Long> processor,
                                   Object context, int successRc,
                                   int failureRc) {
            this.level = level;
            this.path = path;
            this.processor = processor;
            this.context = context;
            this.successRc = successRc;
            this.failureRc = failureRc;
        }

        @Override
        public void process(String lNode, AsyncCallback.VoidCallback cb) {
            String nodePath = path + "/" + lNode;
            if ((level == 0) && !isLedgerParentNode(lNode)) {
                cb.processResult(successRc, null, context);
                return;
            } else if (level < 3) {
                asyncProcessLevelNodes(nodePath,
                        new RecursiveProcessor(level + 1, nodePath, processor, context, successRc, failureRc), cb,
                        context, successRc, failureRc);
            } else {
                // process each ledger after all ledger are processed, cb will be call to continue processing next
                // level4 node
                asyncProcessLedgersInSingleNode(nodePath, processor, cb, context, successRc, failureRc);
            }
        }
    }

    @Override
    protected String getLedgerParentNodeRegex() {
        return StringUtils.LONGHIERARCHICAL_LEDGER_PARENT_NODE_REGEX;
    }

}
