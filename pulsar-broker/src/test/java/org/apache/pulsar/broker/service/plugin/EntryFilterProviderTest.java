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

import static org.apache.pulsar.broker.service.plugin.EntryFilterProvider.ENTRY_FILTER_DEFINITION_FILE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.pulsar.common.nar.NarClassLoader;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

@Test(groups = "broker")
public class EntryFilterProviderTest {

    @Test
    public void testReadYamlFile() throws IOException {
        try (NarClassLoader cl = mock(NarClassLoader.class)) {
            when(cl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE + ".yaml"))
                    .thenThrow(new NoSuchFileException(""));
            try {
                EntryFilterProvider.getEntryFilterDefinition(cl);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertFalse(e instanceof NoSuchFileException);
            }
        }
        try (NarClassLoader cl = mock(NarClassLoader.class)) {
            when(cl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE + ".yml"))
                    .thenThrow(new NoSuchFileException(""));
            try {
                EntryFilterProvider.getEntryFilterDefinition(cl);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertFalse(e instanceof NoSuchFileException);
            }
        }
        try (NarClassLoader cl = mock(NarClassLoader.class)) {
            when(cl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE + ".yaml"))
                    .thenThrow(new NoSuchFileException(""));
            when(cl.getServiceDefinition(ENTRY_FILTER_DEFINITION_FILE + ".yml"))
                    .thenThrow(new NoSuchFileException(""));
            try {
                EntryFilterProvider.getEntryFilterDefinition(cl);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof NoSuchFileException);
            }
        }
    }
}
