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

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;

@UtilityClass
@Slf4j
class HierarchicalLedgerUtils {
    /**
     * Get all ledger ids in the given zk path.
     *
     * @param ledgerNodes
     *          List of ledgers in the given path
     *          example:- {L1652, L1653, L1650}
     * @param path
     *          The zookeeper path of the ledger ids. The path should start with {@ledgerRootPath}
     *          example (with ledgerRootPath = /ledgers):- /ledgers/00/0053
     */
    NavigableSet<Long> ledgerListToSet(List<String> ledgerNodes, String ledgerRootPath, String path) {
        NavigableSet<Long> zkActiveLedgers = new TreeSet<>();

        if (!path.startsWith(ledgerRootPath)) {
            log.warn("Ledger path [{}] is not a valid path name, it should start wth {}", path, ledgerRootPath);
            return zkActiveLedgers;
        }

        long ledgerIdPrefix = 0;
        char ch;
        for (int i = ledgerRootPath.length() + 1; i < path.length(); i++) {
            ch = path.charAt(i);
            if (ch < '0' || ch > '9') {
                continue;
            }
            ledgerIdPrefix = ledgerIdPrefix * 10 + (ch - '0');
        }

        for (String ledgerNode : ledgerNodes) {
            if (AbstractZkLedgerManager.isSpecialZnode(ledgerNode)) {
                continue;
            }
            long ledgerId = ledgerIdPrefix;
            for (int i = 0; i < ledgerNode.length(); i++) {
                ch = ledgerNode.charAt(i);
                if (ch < '0' || ch > '9') {
                    continue;
                }
                ledgerId = ledgerId * 10 + (ch - '0');
            }
            zkActiveLedgers.add(ledgerId);
        }
        return zkActiveLedgers;
    }
}
