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
package org.apache.bookkeeper.mledger.impl.cache;

import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.pulsar.broker.PulsarService;

public class RangeCacheTestUtil {
    /**
     * Iterates over all cached entries in the RangeEntryCacheManager and applies the provided consumer to each entry.
     *
     * @param pulsarService the PulsarService instance to access the entry cache manager
     * @param consumer      the consumer to apply to each cached entry
     */
    public static void forEachCachedEntry(PulsarService pulsarService,
                                          Consumer<ReferenceCountedEntry> consumer) {
        RangeEntryCacheManagerImpl entryCacheManagerImpl =
                (RangeEntryCacheManagerImpl) pulsarService.getDefaultManagedLedgerFactory().getEntryCacheManager();
        entryCacheManagerImpl.forEachEntry(entry -> consumer.accept(entry.value));
    }
}
