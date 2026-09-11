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
package org.apache.pulsar.client.api;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

public abstract class AbstractMessageDispatchThrottlingTest extends ProducerConsumerBase {
    public static <T> T[] merge(T[] first, T[] last) {
        int totalLength = first.length + last.length;
        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        System.arraycopy(last, 0, result, offset, first.length);
        return result;
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        internalSetup();
        producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    protected void reset() throws Exception {
        pulsar.getConfiguration().setForceDeleteTenantAllowed(true);
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

        for (String tenant : admin.tenants().getTenants()) {
            for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                admin.namespaces().deleteNamespace(namespace, true);
            }
            admin.tenants().deleteTenant(tenant, true);
        }

        for (String cluster : admin.clusters().getClusters()) {
            admin.clusters().deleteCluster(cluster);
        }

        pulsar.getConfiguration().setForceDeleteTenantAllowed(false);
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);

        producerBaseSetup();
    }

    @DataProvider(name = "subscriptions")
    public Object[][] subscriptionsProvider() {
        return new Object[][]{new Object[]{SubscriptionType.Shared}, {SubscriptionType.Exclusive}};
    }

    @DataProvider(name = "dispatchRateType")
    public Object[][] dispatchRateProvider() {
        return new Object[][]{{DispatchRateType.messageRate}, {DispatchRateType.byteRate}};
    }

    @DataProvider(name = "subscriptionAndDispatchRateType")
    public Object[][] subDisTypeProvider() {
        List<Object[]> mergeList = new LinkedList<>();
        for (Object[] sub : subscriptionsProvider()) {
            for (Object[] dispatch : dispatchRateProvider()) {
                mergeList.add(AbstractMessageDispatchThrottlingTest.merge(sub, dispatch));
            }
        }
        return mergeList.toArray(new Object[0][0]);
    }

    protected void deactiveCursors(ManagedLedgerImpl ledger) throws Exception {
        Field statsUpdaterField = BrokerService.class.getDeclaredField("statsUpdater");
        statsUpdaterField.setAccessible(true);
        ScheduledExecutorService statsUpdater = (ScheduledExecutorService) statsUpdaterField
                .get(pulsar.getBrokerService());
        statsUpdater.shutdownNow();
        ledger.getCursors().forEach(cursor -> {
            ledger.deactivateCursor(cursor);
        });
    }

    enum DispatchRateType {
        messageRate, byteRate;
    }
}
