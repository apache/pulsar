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

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ExtensibleLoadManagerImplWithTransactionCoordinatorTest extends ExtensibleLoadManagerImplBaseTest {

    public ExtensibleLoadManagerImplWithTransactionCoordinatorTest() {
        super("public/test-elb-with-tx");
    }

    @Override
    protected ServiceConfiguration updateConfig(ServiceConfiguration conf) {
        conf = super.updateConfig(conf);
        conf.setTransactionCoordinatorEnabled(true);
        return conf;
    }

    @Test(timeOut = 30 * 1000)
    public void testUnloadAdminAPI() throws Exception {
        var topicAndBundle = getBundleIsNotOwnByChangeEventTopic("test-unload");
        var topicName = topicAndBundle.getLeft();
        var bundle = topicAndBundle.getRight();

        var srcBroker = admin.lookups().lookupTopic(topicName.toString());
        var dstBroker = srcBroker.equals(pulsar1.getBrokerServiceUrl()) ? pulsar2 : pulsar1;
        var dstBrokerUrl = dstBroker.getBrokerId();
        var dstBrokerServiceUrl = dstBroker.getBrokerServiceUrl();

        admin.namespaces().unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange(), dstBrokerUrl);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.lookups().lookupTopic(topicName.toString()), dstBrokerServiceUrl));
    }
}
