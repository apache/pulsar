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
package org.apache.pulsar.broker.service.plugin;

import org.apache.bookkeeper.mledger.Entry;

public interface EntryFilter {

    /**
     * 1. Broker determines whether to filter out this entry based on the return value of this method.
     * 2. Do not deserialize the entire entry in this method,
     * which has a great impact on the broker's memory and CPU.
     * 3. Return ACCEPT or null will be regarded as ACCEPT.
     * @param entry
     * @param context
     * @return
     */
    FilterResult filterEntry(Entry entry, FilterContext context);

    /**
     * close the entry filter.
     */
    void close();


    enum FilterResult {
        /**
         * deliver to the consumer.
         */
        ACCEPT,
        /**
         * skip the message.
         */
        REJECT,
    }

}
