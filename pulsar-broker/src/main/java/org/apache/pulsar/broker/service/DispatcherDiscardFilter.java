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

package org.apache.pulsar.broker.service;

import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * Callback to determine if an entry should be discarded by the dispatcher.
 */
public interface DispatcherDiscardFilter {
    /**
     * The dispatcher will release the entry and not dispatch it if this filter returns true.
     *
     * @param consumer The consumer that is consuming the message
     * @param entry The entry to be dispatched
     * @param msgMetadata The metadata of the message
     * @param isReplayRead If the entry is being read during a replay
     * @param unackedMessagesInEntry The number of unacked messages in the entry batch
     * @return true if the entry should be discarded, false otherwise
     */
    boolean shouldDiscardEntry(Consumer consumer, Entry entry,
                               MessageMetadata msgMetadata, boolean isReplayRead,
                               Supplier<Integer> unackedMessagesInEntry);
}
