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
package org.apache.pulsar.client.impl;

import com.google.common.collect.Sets;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class SchemaDeleteTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {

        super.internalSetup();
        this.conf.setBrokerDeleteInactiveTopicsFrequencySeconds(5);

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void createTopicDeleteTopicCreateTopic() throws Exception {
        String namespace = "my-property/my-ns";
        String topic = namespace + "/topic1";
        String foobar = "foo";

        try (Producer<String> producer =
                pulsarClient.newProducer(Schema.STRING).topic(topic).create()) {
            producer.send(foobar);
        }

        admin.topics().delete(topic);
        admin.schemas().deleteSchema(topic);

        // creating a subscriber will check the schema against the latest
        // schema, which in this case should be a tombstone, which should
        // behave as if the schema never existed
        try (Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topic).startMessageId(MessageId.latest).create()) {
        }

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                SchemaAutoUpdateCompatibilityStrategy.BackwardTransitive);
        admin.topics().delete(topic);
        admin.schemas().deleteSchema(topic);

        // with a transitive policy we should check all previous schemas. But we
        // shouldn't check against those that were there before we deleted the schema.
        try (Reader<DummyPojo> reader = pulsarClient.newReader(Schema.AVRO(DummyPojo.class))
                .topic(topic).startMessageId(MessageId.latest).create()) {
        }
    }

    public static class DummyPojo {
        int foobar;
    }
}
