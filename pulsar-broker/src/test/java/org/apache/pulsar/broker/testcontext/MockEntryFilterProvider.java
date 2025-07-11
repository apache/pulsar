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
package org.apache.pulsar.broker.testcontext;

import lombok.SneakyThrows;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.plugin.EntryFilterDefinition;
import org.apache.pulsar.broker.service.plugin.EntryFilterMetaData;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.common.nar.NarClassLoader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockEntryFilterProvider extends EntryFilterProvider {

    public MockEntryFilterProvider(ServiceConfiguration config) throws IOException {
        super(config);
    }

    @SneakyThrows
    public void setMockEntryFilters(EntryFilterDefinition... defs) {
        definitions = new HashMap<>();
        cachedClassLoaders = new HashMap<>();
        brokerEntryFilters = new ArrayList<>();

        for (EntryFilterDefinition def : defs) {
            final String name = def.getName();
            final EntryFilterMetaData meta = new EntryFilterMetaData();
            meta.setDefinition(def);
            meta.setArchivePath(Path.of(name));
            definitions.put(name, meta);
            final NarClassLoader ncl = mock(NarClassLoader.class);

            when(ncl.loadClass(anyString())).thenAnswer(a -> {
                final Object argument = a.getArguments()[0];
                return Thread.currentThread().getContextClassLoader().loadClass(argument.toString());
            });
            cachedClassLoaders.put(Path.of(name).toString(), ncl);
        }
        initializeBrokerEntryFilters();
    }

}
