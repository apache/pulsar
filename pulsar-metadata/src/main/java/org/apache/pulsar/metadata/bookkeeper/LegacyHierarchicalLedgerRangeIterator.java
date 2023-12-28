/*
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver.BLOCKING_CALL_TIMEOUT;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStore;


/**
 * Hierarchical Ledger Manager which manages ledger meta in zookeeper using 2-level hierarchical znodes.
 *
 * <p>LegacyHierarchicalLedgerManager splits the generated id into 3 parts (2-4-4):
 * <pre>&lt;level1 (2 digits)&gt;&lt;level2 (4 digits)&gt;&lt;level3 (4 digits)&gt;</pre>
 * These 3 parts are used to form the actual ledger node path used to store ledger metadata:
 * <pre>(ledgersRootPath)/level1/level2/L(level3)</pre>
 * E.g Ledger 0000000001 is split into 3 parts <i>00</i>, <i>0000</i>, <i>0001</i>, which is stored in
 * <i>(ledgersRootPath)/00/0000/L0001</i>. So each znode could have at most 10000 ledgers, which avoids
 * errors during garbage collection due to lists of children that are too long.
 */
@Slf4j
public class LegacyHierarchicalLedgerRangeIterator implements LedgerManager.LedgerRangeIterator {

    private static final String MAX_ID_SUFFIX = "9999";
    private static final String MIN_ID_SUFFIX = "0000";


    private final MetadataStore store;
    private final String ledgersRoot;

    private Iterator<String> l1NodesIter = null;
    private Iterator<String> l2NodesIter = null;
    private String curL1Nodes = "";
    private boolean iteratorDone = false;
    private LedgerManager.LedgerRange nextRange = null;

    public LegacyHierarchicalLedgerRangeIterator(MetadataStore store, String ledgersRoot) {
        this.store = store;
        this.ledgersRoot = ledgersRoot;
    }

    /**
     * Iterate next level1 znode.
     *
     * @return false if have visited all level1 nodes
     * @throws InterruptedException/KeeperException if error occurs reading zookeeper children
     */
    private boolean nextL1Node() throws ExecutionException, InterruptedException, TimeoutException {
        l2NodesIter = null;
        while (l2NodesIter == null) {
            if (l1NodesIter.hasNext()) {
                curL1Nodes = l1NodesIter.next();
            } else {
                return false;
            }
            // Top level nodes are always exactly 2 digits long. (Don't pick up long hierarchical top level nodes)
            if (!isLedgerParentNode(curL1Nodes)) {
                continue;
            }
            List<String> l2Nodes = store.getChildren(ledgersRoot + "/" + curL1Nodes)
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
            l2NodesIter = l2Nodes.iterator();
            if (!l2NodesIter.hasNext()) {
                l2NodesIter = null;
                continue;
            }
        }
        return true;
    }

    private synchronized void preload() throws IOException {
        while (nextRange == null && !iteratorDone) {
            boolean hasMoreElements = false;
            try {
                if (l1NodesIter == null) {
                    List<String> l1Nodes = store.getChildren(ledgersRoot)
                            .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
                    l1NodesIter = l1Nodes.iterator();
                    hasMoreElements = nextL1Node();
                } else if (l2NodesIter == null || !l2NodesIter.hasNext()) {
                    hasMoreElements = nextL1Node();
                } else {
                    hasMoreElements = true;
                }
            } catch (ExecutionException | TimeoutException ke) {
                throw new IOException("Error preloading next range", ke);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while preloading", ie);
            }
            if (hasMoreElements) {
                nextRange = getLedgerRangeByLevel(curL1Nodes, l2NodesIter.next());
                if (nextRange.size() == 0) {
                    nextRange = null;
                }
            } else {
                iteratorDone = true;
            }
        }
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        preload();
        return nextRange != null && !iteratorDone;
    }

    @Override
    public synchronized LedgerManager.LedgerRange next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        LedgerManager.LedgerRange r = nextRange;
        nextRange = null;
        return r;
    }

    private static final ThreadLocal<StringBuilder> threadLocalNodeBuilder =
            ThreadLocal.withInitial(() -> new StringBuilder());

    /**
     * Get a single node level1/level2.
     *
     * @param level1
     *          1st level node name
     * @param level2
     *          2nd level node name
     * @throws IOException
     */
    LedgerManager.LedgerRange getLedgerRangeByLevel(final String level1, final String level2)
            throws IOException {
        StringBuilder nodeBuilder = threadLocalNodeBuilder.get();
        nodeBuilder.setLength(0);
        nodeBuilder.append(ledgersRoot).append("/")
                .append(level1).append("/").append(level2);
        String nodePath = nodeBuilder.toString();
        List<String> ledgerNodes = null;
        try {
            ledgerNodes = store.getChildren(nodePath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Error when get child nodes from zk", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Error when get child nodes from zk", e);
        }
        NavigableSet<Long> zkActiveLedgers =
                HierarchicalLedgerUtils.ledgerListToSet(ledgerNodes, ledgersRoot, nodePath);
        if (log.isDebugEnabled()) {
            log.debug("All active ledgers from ZK for hash node "
                    + level1 + "/" + level2 + " : " + zkActiveLedgers);
        }

        return new LedgerManager.LedgerRange(zkActiveLedgers.subSet(getStartLedgerIdByLevel(level1, level2), true,
                getEndLedgerIdByLevel(level1, level2), true));
    }

    /**
     * Get the smallest cache id in a specified node /level1/level2.
     *
     * @param level1
     *          1st level node name
     * @param level2
     *          2nd level node name
     * @return the smallest ledger id
     */
    private long getStartLedgerIdByLevel(String level1, String level2) throws IOException {
        return StringUtils.stringToHierarchicalLedgerId(level1, level2, MIN_ID_SUFFIX);
    }

    /**
     * Get the largest cache id in a specified node /level1/level2.
     *
     * @param level1
     *          1st level node name
     * @param level2
     *          2nd level node name
     * @return the largest ledger id
     */
    private long getEndLedgerIdByLevel(String level1, String level2) throws IOException {
        return StringUtils.stringToHierarchicalLedgerId(level1, level2, MAX_ID_SUFFIX);
    }

    private boolean isLedgerParentNode(String path) {
        return path.matches(StringUtils.LEGACYHIERARCHICAL_LEDGER_PARENT_NODE_REGEX);
    }
}
