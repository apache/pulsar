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

import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.zookeeper.AsyncCallback;

class LegacyHierarchicalLedgerManager extends AbstractHierarchicalLedgerManager {

    LegacyHierarchicalLedgerManager(MetadataStore store,
                                    ScheduledExecutorService scheduler,
                                    String ledgerRootPath) {
        super(store, scheduler, ledgerRootPath);
    }

    @Override
    protected String getLedgerParentNodeRegex() {
        return StringUtils.LEGACYHIERARCHICAL_LEDGER_PARENT_NODE_REGEX;
    }

    public void asyncProcessLedgers(final BookkeeperInternalCallbacks.Processor<Long> processor,
                                    final AsyncCallback.VoidCallback finalCb, final Object context,
                                    final int successRc, final int failureRc) {
        // process 1st level nodes
        asyncProcessLevelNodes(ledgerRootPath, (l1Node, cb1) -> {
            if (!isLedgerParentNode(l1Node)) {
                cb1.processResult(successRc, null, context);
                return;
            }

            String l1NodePath = ledgerRootPath + "/" + l1Node;
            // process level1 path, after all children of level1 process
            // it callback to continue processing next level1 node
            asyncProcessLevelNodes(l1NodePath, (l2Node, cb2) -> {
                // process level1/level2 path
                String l2NodePath = ledgerRootPath + "/" + l1Node + "/" + l2Node;
                // process each ledger
                // after all ledger are processed, cb2 will be call to continue processing next level2 node
                asyncProcessLedgersInSingleNode(l2NodePath, processor, cb2,
                        context, successRc, failureRc);
            }, cb1, context, successRc, failureRc);
        }, finalCb, context, successRc, failureRc);
    }
}
