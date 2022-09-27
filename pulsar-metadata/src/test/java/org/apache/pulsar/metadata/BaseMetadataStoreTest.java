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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdClusterFactory;
import java.io.File;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.pulsar.tests.TestRetrySupport;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

public abstract class BaseMetadataStoreTest extends TestRetrySupport {
    protected TestZKServer zks;
    protected EtcdCluster etcdCluster;

    @BeforeClass(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        incrementSetupNumber();
        zks = new TestZKServer();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        if (zks != null) {
            zks.close();
            zks = null;
        }

        if (etcdCluster != null) {
            etcdCluster.close();
            etcdCluster = null;
        }
    }

    private static String createTempFolder() {
        File temp = Files.newTemporaryFolder();
        temp.deleteOnExit();
        return temp.getAbsolutePath();
    }

    @DataProvider(name = "impl")
    public Object[][] implementations() {
        // A Supplier<String> must be used for the Zookeeper connection string parameter. The retried test run will
        // use the same arguments as the failed attempt.
        // The Zookeeper test server gets restarted by TestRetrySupport before the retry.
        // The new connection string won't be available to the test method unless a
        // Supplier<String> lambda is used for providing the value.
        return new Object[][]{
                {"ZooKeeper", stringSupplier(() -> zks.getConnectionString())},
                {"Memory", stringSupplier(() -> "memory:" + UUID.randomUUID())},
                {"RocksDB", stringSupplier(() -> "rocksdb:" + createTempFolder())},
                {"Etcd", stringSupplier(() -> "etcd:" + getEtcdClusterConnectString())},
        };
    }

    private synchronized String getEtcdClusterConnectString() {
        if (etcdCluster == null) {
            etcdCluster = EtcdClusterFactory.buildCluster("test", 1, false);
            etcdCluster.start();
        }
        return etcdCluster.getClientEndpoints().stream().map(URI::toString).collect(Collectors.joining(","));
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

    public static void assertEqualsAndRetry(Supplier<Object> actual,
                                            Object expected,
                                            Object expectedAndRetry) throws Exception {
        assertEqualsAndRetry(actual, expected, expectedAndRetry, 5, 100);
    }

    public static void assertEqualsAndRetry(Supplier<Object> actual,
                                            Object expected,
                                            Object expectedAndRetry,
                                            int retryCount,
                                            long intSleepTimeInMillis) throws Exception {
        assertTrue(retryStrategically((__) -> {
            if (actual.get().equals(expectedAndRetry)) {
                return false;
            }
            assertEquals(actual.get(), expected);
            return true;
        }, retryCount, intSleepTimeInMillis));
    }

    public static boolean retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
            throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null)) {
                return true;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
        return false;
    }
}
