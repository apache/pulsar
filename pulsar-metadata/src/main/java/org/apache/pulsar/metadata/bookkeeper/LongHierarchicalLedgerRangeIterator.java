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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStore;

/**
 * Iterates recursively through each metadata bucket.
 */
@Slf4j
class LongHierarchicalLedgerRangeIterator implements LedgerManager.LedgerRangeIterator {

    private final MetadataStore store;
    private final String ledgerRootPath;
    LedgerManager.LedgerRangeIterator rootIterator;


    LongHierarchicalLedgerRangeIterator(MetadataStore store, String ledgerRootPath) {
        this.store = store;
        this.ledgerRootPath = ledgerRootPath;
    }

    /**
     * Returns all children with path as a parent.  If path is non-existent,
     * returns an empty list anyway (after all, there are no children there).
     * Maps all exceptions (other than NoNode) to IOException in keeping with
     * LedgerRangeIterator.
     *
     * @param path
     * @return Iterator into set of all children with path as a parent
     * @throws IOException
     */
    List<String> getChildrenAt(String path) throws IOException {
        try {
            return store.sync(path).thenCompose(__ -> store.getChildrenFromStore(path))
                    .get(AbstractMetadataDriver.BLOCKING_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to get children at {}", path);
            }
            throw new IOException(e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading ledgers at path " + path, ie);
        }
    }

    /**
     * Represents the ledger range rooted at a leaf node, returns at most one LedgerRange.
     */
    class LeafIterator implements LedgerManager.LedgerRangeIterator {
        // Null iff iteration is complete
        LedgerManager.LedgerRange range;

        LeafIterator(String path) throws IOException {
            List<String> ledgerLeafNodes = getChildrenAt(path);
            Set<Long> ledgerIds = HierarchicalLedgerUtils.ledgerListToSet(ledgerLeafNodes, ledgerRootPath, path);
            if (log.isDebugEnabled()) {
                log.debug("All active ledgers from ZK for hash node {}: {}", path, ledgerIds);
            }
            if (!ledgerIds.isEmpty()) {
                range = new LedgerManager.LedgerRange(ledgerIds);
            } // else, hasNext() should return false so that advance will skip us and move on
        }

        @Override
        public boolean hasNext() throws IOException {
            return range != null;
        }

        @Override
        public LedgerManager.LedgerRange next() throws IOException {
            if (range == null) {
                throw new NoSuchElementException(
                        "next() must only be called if hasNext() is true");
            }
            LedgerManager.LedgerRange ret = range;
            range = null;
            return ret;
        }
    }


    /**
     * The main constraint is that between calls one of two things must be true.
     * 1) nextLevelIterator is null and thisLevelIterator.hasNext() == false: iteration complete, hasNext()
     *    returns false
     * 2) nextLevelIterator is non-null: nextLevelIterator.hasNext() must return true and nextLevelIterator.next()
     *    must return the next LedgerRange
     * The above means that nextLevelIterator != null ==> nextLevelIterator.hasNext()
     * It also means that hasNext() iff nextLevelIterator != null
     */
    private class InnerIterator implements LedgerManager.LedgerRangeIterator {
        final String path;
        final int level;

        // Always non-null
        final Iterator<String> thisLevelIterator;
        // non-null iff nextLevelIterator.hasNext() is true
        LedgerManager.LedgerRangeIterator nextLevelIterator;

        /**
         * Builds InnerIterator.
         *
         * @param path Subpath for thisLevelIterator
         * @param level Level of thisLevelIterator (must be <= 3)
         * @throws IOException
         */
        InnerIterator(String path, int level) throws IOException {
            this.path = path;
            this.level = level;
            thisLevelIterator = getChildrenAt(path).iterator();
            advance();
        }

        /**
         * Resolves the difference between cases 1 and 2 after nextLevelIterator is exhausted.
         * Pre-condition: nextLevelIterator == null, thisLevelIterator != null
         * Post-condition: nextLevelIterator == null && !thisLevelIterator.hasNext() OR
         *                 nextLevelIterator.hasNext() == true and nextLevelIterator.next()
         *                 yields the next result of next()
         * @throws IOException Exception representing error
         */
        void advance() throws IOException {
            while (thisLevelIterator.hasNext()) {
                String node = thisLevelIterator.next();
                if (level == 0 && !isLedgerParentNode(node)) {
                    continue;
                }
                LedgerManager.LedgerRangeIterator nextIterator = level < 3
                        ? new InnerIterator(path + "/" + node, level + 1)
                        : new LeafIterator(path + "/" + node);
                if (nextIterator.hasNext()) {
                    nextLevelIterator = nextIterator;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() throws IOException {
            return nextLevelIterator != null;
        }

        @Override
        public LedgerManager.LedgerRange next() throws IOException {
            LedgerManager.LedgerRange ret = nextLevelIterator.next();
            if (!nextLevelIterator.hasNext()) {
                nextLevelIterator = null;
                advance();
            }
            return ret;
        }
    }

    private void bootstrap() throws IOException {
        if (rootIterator == null) {
            rootIterator = new InnerIterator(ledgerRootPath, 0);
        }
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        bootstrap();
        return rootIterator.hasNext();
    }

    @Override
    public synchronized LedgerManager.LedgerRange next() throws IOException {
        bootstrap();
        return rootIterator.next();
    }

    /**
     * whether the child of ledgersRootPath is a top level parent znode for
     * ledgers (in HierarchicalLedgerManager) or znode of a ledger (in
     * FlatLedgerManager).
     *
     */
    public boolean isLedgerParentNode(String path) {
        return path.matches(StringUtils.LONGHIERARCHICAL_LEDGER_PARENT_NODE_REGEX);
    }
}