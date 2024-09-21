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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.apache.pulsar.common.util.PortManager.nextLockedFreePort;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Unit test for {@link ExtensibleLoadManagerImpl with AdvertisedListeners broker configs}.
 */
@Slf4j
@Test(groups = "flaky")
@SuppressWarnings("unchecked")
public class ExtensibleLoadManagerImplWithAdvertisedListenersTest extends ExtensibleLoadManagerImplBaseTest {

    public String brokerServiceUrl;

    @Factory(dataProvider = "serviceUnitStateTableViewClassName")
    public ExtensibleLoadManagerImplWithAdvertisedListenersTest(String serviceUnitStateTableViewClassName) {
        super("public/test", serviceUnitStateTableViewClassName);
    }

    @Override
    protected ServiceConfiguration updateConfig(ServiceConfiguration conf) {
        super.updateConfig(conf);
        int privatePulsarPort = nextLockedFreePort();
        int publicPulsarPort = nextLockedFreePort();
        conf.setInternalListenerName("internal");
        conf.setBindAddresses("external:pulsar://localhost:" + publicPulsarPort);
        conf.setAdvertisedListeners(
                "external:pulsar://localhost:" + publicPulsarPort +
                        ",internal:pulsar://localhost:" + privatePulsarPort);
        conf.setWebServicePortTls(Optional.empty());
        conf.setBrokerServicePortTls(Optional.empty());
        conf.setBrokerServicePort(Optional.of(privatePulsarPort));
        conf.setWebServicePort(Optional.of(0));
        brokerServiceUrl = conf.getBindAddresses().replaceAll("external:", "");
        return conf;
    }

    @DataProvider(name = "isPersistentTopicSubscriptionTypeTest")
    public Object[][] isPersistentTopicSubscriptionTypeTest() {
        return new Object[][]{
                {TopicDomain.non_persistent, SubscriptionType.Exclusive},
                {TopicDomain.persistent, SubscriptionType.Key_Shared}
        };
    }

    @Test(timeOut = 30_000, dataProvider = "isPersistentTopicSubscriptionTypeTest")
    public void testTransferClientReconnectionWithoutLookup(TopicDomain topicDomain, SubscriptionType subscriptionType)
            throws Exception {
        ExtensibleLoadManagerImplTest.testTransferClientReconnectionWithoutLookup(
                clients,
                topicDomain, subscriptionType,
                defaultTestNamespace, admin,
                brokerServiceUrl,
                pulsar1, pulsar2, primaryLoadManager, secondaryLoadManager);
    }

    @Test(timeOut = 30 * 1000, dataProvider = "isPersistentTopicSubscriptionTypeTest")
    public void testUnloadClientReconnectionWithLookup(TopicDomain topicDomain,
                                                       SubscriptionType subscriptionType) throws Exception {
        ExtensibleLoadManagerImplTest.testUnloadClientReconnectionWithLookup(
                clients,
                topicDomain, subscriptionType,
                defaultTestNamespace, admin,
                brokerServiceUrl,
                pulsar1);
    }

    @DataProvider(name = "isPersistentTopicTest")
    public Object[][] isPersistentTopicTest() {
        return new Object[][]{{TopicDomain.persistent}, {TopicDomain.non_persistent}};
    }

    @Test(timeOut = 30 * 1000, dataProvider = "isPersistentTopicTest")
    public void testOptimizeUnloadDisable(TopicDomain topicDomain) throws Exception {
        ExtensibleLoadManagerImplTest.testOptimizeUnloadDisable(
                clients,
                topicDomain, defaultTestNamespace, admin,
                brokerServiceUrl, pulsar1, pulsar2);
    }

}
