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
package org.apache.pulsar.broker.transaction.coordinator;

import com.google.common.hash.Hashing;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Unit tests for transaction coordinator assign
 */
public class TransactionCoordinatorAssignTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTCAssignTopicNamespaceBootstrap() throws Exception {
        final List<NamespaceBundle> loadedBundles = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        pulsar.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {

            @Override
            public void onLoad(NamespaceBundle bundle) {
                loadedBundles.add(bundle);
                latch.countDown();
            }

            @Override
            public void unLoad(NamespaceBundle bundle) {
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return namespaceBundle.getNamespaceObject().equals(NamespaceName.SYSTEM_NAMESPACE);
            }
        });

        Assert.assertEquals(loadedBundles.size(), 1);
        Assert.assertEquals(loadedBundles.get(0).getNamespaceObject(), NamespaceName.SYSTEM_NAMESPACE);
        Assert.assertEquals(loadedBundles.get(0),
                NamespaceBundleFactory.createFactory(pulsar, Hashing.crc32())
                        .getFullBundle(loadedBundles.get(0).getNamespaceObject()));
    }
}
