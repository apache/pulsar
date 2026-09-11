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

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;

/**
 * Function that computes the length of an entry.
 * The reason for adding this interface is to allow testing caching scenarios with large entry sizes
 * without having to test with actual real entries that match the larger size.
 * This simplifies the testing of the cache when it's necessary to simulate a scenario where the cache size
 * becomes a limiting factor in cache efficiency.
 */
@FunctionalInterface
public interface EntryLengthFunction {
    EntryLengthFunction DEFAULT = (ml, entry) -> entry.getLength();
    int getEntryLength(ManagedLedgerImpl ml, Entry entry);
}
