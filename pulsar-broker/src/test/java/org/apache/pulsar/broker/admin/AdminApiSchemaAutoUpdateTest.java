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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.reflect.AvroAlias;
import org.apache.avro.reflect.AvroDefault;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApiSchemaAutoUpdateTest extends MockedPulsarServiceBaseTest {
    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
        admin.namespaces().createNamespace("prop-xyz/test/ns1");
        admin.namespaces().createNamespace("prop-xyz/ns2", Sets.newHashSet("test"));
        admin.namespaces().createNamespace("prop-xyz/test/ns2");
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private void testAutoUpdateBackward(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                                                                    SchemaAutoUpdateCompatibilityStrategy.Backward);

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }

        log.info("try with forward compat, should fail");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            Assert.fail("Forward compat schema should be rejected");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }

        log.info("try with backward compat, should succeed");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            p.send(new V2Data("test2"));
        }

    }

    private void testAutoUpdateForward(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                                                                    SchemaAutoUpdateCompatibilityStrategy.Forward);

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }

        log.info("try with backward compat, should fail");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            Assert.fail("Backward compat schema should be rejected");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }

        log.info("try with forward compat, should succeed");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            p.send(new V3Data("test2", 1, 2));
        }
    }

    private void testAutoUpdateFull(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }

        log.info("try with backward compat only, should fail");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            Assert.fail("Backward compat only schema should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }

        log.info("try with forward compat only, should fail");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            Assert.fail("Forward compat only schema should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }

        log.info("try with fully compat");
        try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
            p.send(new V4Data("test2", 1, (short)100));
        }
    }

    private void testAutoUpdateDisabled(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled);

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }
        log.info("try with backward compat only, should fail");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            Assert.fail("Backward compat only schema should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }

        log.info("try with forward compat only, should fail");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            Assert.fail("Forward compat only schema should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }

        log.info("try with fully compat, should fail");
        try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
            Assert.fail("Fully compat schema should fail, autoupdate disabled");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }

        log.info("Should still be able to connect with original schema");
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test2", 2));
        }

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                SchemaAutoUpdateCompatibilityStrategy.Full);

        Awaitility.await().untilAsserted(
                () -> Assert.assertEquals(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace),
                        SchemaAutoUpdateCompatibilityStrategy.Full));

        log.info("try with fully compat, again");
        try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
            p.send(new V4Data("test2", 1, (short)100));
        }
    }

    @AvroAlias(space="blah", alias="data")
    static class V1Data {
        String foo;
        int bar;

        V1Data(String foo, int bar) {
            this.foo = foo;
            this.bar = bar;
        }
    }

    // backward compatible with V1Data
    @AvroAlias(space="blah", alias="data")
    static class V2Data {
        String foo;

        V2Data(String foo) {
            this.foo = foo;
        }
    }

    // forward compatible with V1Data
    @AvroAlias(space="blah", alias="data")
    static class V3Data {
        String foo;
        int bar;
        long baz;

        V3Data(String foo, int bar, long baz) {
            this.foo = foo;
            this.bar = bar;
            this.baz = baz;
        }
    }

    // fully compatible with V1Data
    @AvroAlias(space="blah", alias="data")
    static class V4Data {
        String foo;
        int bar;
        @AvroDefault(value = "10")
        short blah;

        V4Data(String foo, int bar, short blah) {
            this.foo = foo;
            this.bar = bar;
            this.blah = blah;
        }
    }

    @Test
    public void testBackwardV2() throws Exception {
        testAutoUpdateBackward("prop-xyz/ns1", "persistent://prop-xyz/ns1/backward");
        testAutoUpdateBackward("prop-xyz/ns2", "non-persistent://prop-xyz/ns2/backward-np");
    }

    @Test
    public void testForwardV2() throws Exception {
        testAutoUpdateForward("prop-xyz/ns1", "persistent://prop-xyz/ns1/forward");
        testAutoUpdateForward("prop-xyz/ns2", "non-persistent://prop-xyz/ns2/forward-np");
    }

    @Test
    public void testFullV2() throws Exception {
        testAutoUpdateFull("prop-xyz/ns1", "persistent://prop-xyz/ns1/full");
        testAutoUpdateFull("prop-xyz/ns2", "non-persistent://prop-xyz/ns2/full-np");
    }

    @Test
    public void testDisabledV2() throws Exception {
        testAutoUpdateDisabled("prop-xyz/ns1", "persistent://prop-xyz/ns1/disabled");
        testAutoUpdateDisabled("prop-xyz/ns2", "non-persistent://prop-xyz/ns2/disabled-np");
    }

    @Test
    public void testBackwardV1() throws Exception {
        testAutoUpdateBackward("prop-xyz/test/ns1", "persistent://prop-xyz/test/ns1/backward");
        testAutoUpdateBackward("prop-xyz/test/ns2", "non-persistent://prop-xyz/test/ns2/backward-np");
    }

    @Test
    public void testForwardV1() throws Exception {
        testAutoUpdateForward("prop-xyz/test/ns1", "persistent://prop-xyz/test/ns1/forward");
        testAutoUpdateForward("prop-xyz/test/ns2", "non-persistent://prop-xyz/test/ns2/forward-np");
    }

    @Test
    public void testFullV1() throws Exception {
        testAutoUpdateFull("prop-xyz/test/ns1", "persistent://prop-xyz/test/ns1/full");
        testAutoUpdateFull("prop-xyz/test/ns2", "non-persistent://prop-xyz/test/ns2/full-np");
    }

    @Test
    public void testDisabledV1() throws Exception {
        testAutoUpdateDisabled("prop-xyz/test/ns1", "persistent://prop-xyz/test/ns1/disabled");
        testAutoUpdateDisabled("prop-xyz/test/ns2", "non-persistent://prop-xyz/test/ns2/disabled-np");
    }
}
