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

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.nar.NarClassLoader;

@Slf4j
public class EntryFilterWithClassLoader implements EntryFilter {
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

    @Override
    public void close() {
        entryFilter.close();
        try {
            classLoader.close();
        } catch (IOException e) {
            log.error("close EntryFilterWithClassLoader failed", e);
        }
    }
}
