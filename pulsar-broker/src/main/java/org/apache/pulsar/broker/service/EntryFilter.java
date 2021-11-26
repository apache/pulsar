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
package org.apache.pulsar.broker.service;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.nar.NarClassLoader;

public interface EntryFilter {

    /**
     * Broker will determine whether to filter out this Entry based on the return value of this method.
     * Please do not deserialize the entire Entry in this method,
     * which will have a great impact on Broker's memory and CPU.
     * @param entry
     * @param context
     * @return
     */
    FilterResult filterEntry(Entry entry, FilterContext context);

    @Data
    class FilterContext {

        private EntryBatchSizes batchSizes;
        private SendMessageInfo sendMessageInfo;
        private EntryBatchIndexesAcks indexesAcks;
        private ManagedCursor cursor;
        private boolean isReplayRead;
        private Subscription subscription;
        private SubscriptionOption subscriptionOption;
        private MessageMetadata msgMetadata;

        public void reset() {
            batchSizes = null;
            sendMessageInfo = null;
            indexesAcks = null;
            cursor = null;
            isReplayRead = false;
            subscription = null;
            subscriptionOption = null;
            msgMetadata = null;
        }
    }

    enum FilterResult {
        /**
         * deliver to the Consumer.
         */
        ACCEPT,
        /**
         * skip the message.
         */
        REJECT,
    }

    @Data
    class EntryFilterDefinitions {
        private final Map<String, EntryFilterMetaData> filters = new TreeMap<>();
    }

    @Data
    @NoArgsConstructor
    class EntryFilterMetaData {

        /**
         * The definition of the broker interceptor.
         */
        private EntryFilterDefinition definition;

        /**
         * The path to the handler package.
         */
        private Path archivePath;
    }

    @Data
    @NoArgsConstructor
    class EntryFilterDefinition {

        /**
         * The name of the broker interceptor.
         */
        private String name;

        /**
         * The description of the broker interceptor to be used for user help.
         */
        private String description;

        /**
         * The class name for the broker interceptor.
         */
        private String entryFilterClass;
    }

    class EntryFilterWithClassLoader implements EntryFilter {
        private final EntryFilter entryFilter;
        private final NarClassLoader classLoader;

        public EntryFilterWithClassLoader(EntryFilter entryFilter, NarClassLoader classLoader) {
            this.entryFilter = entryFilter;
            this.classLoader = classLoader;
        }

        @Override
        public FilterResult filterEntry(Entry entry, FilterContext context) {
            return entryFilter.filterEntry(entry, context);
        }
    }
}
