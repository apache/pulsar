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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Properties;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OpReadEntryConfigTest {
    @Test
    public void testDefaultReadCompletionDepth() {
        assertThat(OpReadEntry.readMaxNestedInlineCompletions(new Properties())).isEqualTo(10);
    }

    @DataProvider
    public Object[][] readCompletionDepths() {
        return new Object[][] {
                {"0", 1},
                {"-1", 1},
                {"1", 1},
                {"37", 37},
                {"0x10", 16},
                {"invalid", 10},
                {"", 10},
                {"2147483648", 10}
        };
    }

    @Test(dataProvider = "readCompletionDepths")
    public void testConfiguredReadCompletionDepth(String configuredDepth, int expectedDepth) {
        Properties properties = new Properties();
        properties.setProperty("pulsar.managedLedger.maxReadCompletionDepth", configuredDepth);
        assertThat(OpReadEntry.readMaxNestedInlineCompletions(properties)).isEqualTo(expectedDepth);
    }

    @Test
    public void testLibraryDefaultsToLedgerExecutorAffinity() {
        assertThat(new ManagedLedgerConfig().isReadEntriesCallbackInline()).isFalse();
    }
}
