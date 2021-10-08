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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.apache.pulsar.tests.TestRetrySupport;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

public abstract class BaseMetadataStoreTest extends TestRetrySupport {
    protected TestZKServer zks;

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setup() throws Exception {
        incrementSetupNumber();
        zks = new TestZKServer();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        zks.close();
    }

    @DataProvider(name = "impl")
    public Object[][] implementations() {
        // A Supplier<String> must be used for the Zookeeper connection string parameter. The retried test run will
        // use the same arguments as the failed attempt.
        // The Zookeeper test server gets restarted by TestRetrySupport before the retry.
        // The new connection string won't be available to the test method unless a
        // Supplier<String> lambda is used for providing the value.
        return new Object[][] {
                { "ZooKeeper", stringSupplier(() -> zks.getConnectionString()) },
                { "Memory", stringSupplier(() -> "memory://local") },
        };
    }

    public static Supplier<String> stringSupplier(Supplier<String> supplier) {
        return supplier;
    }

    protected String newKey() {
        return "/key-" + System.nanoTime();
    }

    static void assertException(CompletionException e, Class<?> clazz) {
        assertException(e.getCause(), clazz);
    }

    static void assertException(Throwable t, Class<?> clazz) {
        assertTrue(clazz.isInstance(t), String.format("Exception %s is not of type %s", t.getClass(), clazz));
    }
}
