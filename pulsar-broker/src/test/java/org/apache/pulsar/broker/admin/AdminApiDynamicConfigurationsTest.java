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
package org.apache.pulsar.broker.admin;

import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApiDynamicConfigurationsTest extends MockedPulsarServiceBaseTest {
    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void TestGetAllDynamicConfigurations() throws Exception {
        Map<String,String> configs = admin.brokers().getAllDynamicConfigurations();
        assertNotNull(configs);
    }

    @Test
    public void TestDeleteDynamicConfiguration() throws Exception {
        admin.brokers().deleteDynamicConfiguration("dispatcherMinReadBatchSize");
    }

    @Test
    public void TestDeleteInvalidDynamicConfiguration() {
        try {
            admin.brokers().deleteDynamicConfiguration("errorName");
            fail("exception should be thrown");
        } catch (Exception e) {
            if (e instanceof PulsarAdminException) {
                assertEquals(((PulsarAdminException) e).getStatusCode(), Response.Status.PRECONDITION_FAILED.getStatusCode());
            } else {
                fail("PulsarAdminException should be thrown");
            }
        }
    }

    @Test
    public void testRegisterCustomDynamicConfiguration() throws PulsarAdminException {
        String key = "my-broker-config-key-1";
        String invalidValue = "invalid-value";

        // register
        pulsar.getBrokerService().registerCustomDynamicConfiguration(key, value -> !value.equals(invalidValue));
        assertThrows(IllegalArgumentException.class,
                () -> pulsar.getBrokerService().registerCustomDynamicConfiguration(key, null));
        Map<String, String> allDynamicConfigurations = admin.brokers().getAllDynamicConfigurations();
        assertThat(allDynamicConfigurations).doesNotContainKey(key);

        // update with listener
        AtomicReference<String> changeValue = new AtomicReference<>(null);
        pulsar.getBrokerService().registerConfigurationListener(key, changeValue::set);
        String newValue = "my-broker-config-value-1";
        admin.brokers().updateDynamicConfiguration(key, newValue);
        allDynamicConfigurations = admin.brokers().getAllDynamicConfigurations();
        assertThat(allDynamicConfigurations.get(key)).isEqualTo(newValue);

        Awaitility.await().untilAsserted(() -> {
            assertThat(changeValue.get()).isEqualTo(newValue);
        });

        // update with invalid value
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.brokers().updateDynamicConfiguration(key, invalidValue));

        // delete
        admin.brokers().deleteDynamicConfiguration(key);
        allDynamicConfigurations = admin.brokers().getAllDynamicConfigurations();
        assertThat(allDynamicConfigurations).doesNotContainKey(key);
    }

    @Test
    public void testDeleteStringDynamicConfig() throws PulsarAdminException {
        String syncEventTopic = BrokerTestUtil.newUniqueName(SYSTEM_NAMESPACE + "/tp");
        // The default value is null;
        Awaitility.await().untilAsserted(() -> {
            assertNull(pulsar.getConfig().getConfigurationMetadataSyncEventTopic());
        });
        // Set dynamic config.
        admin.brokers().updateDynamicConfiguration("configurationMetadataSyncEventTopic", syncEventTopic);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsar.getConfig().getConfigurationMetadataSyncEventTopic(), syncEventTopic);
        });
        // Remove dynamic config.
        admin.brokers().deleteDynamicConfiguration("configurationMetadataSyncEventTopic");
        Awaitility.await().untilAsserted(() -> {
            assertNull(pulsar.getConfig().getConfigurationMetadataSyncEventTopic());
        });
    }

    @Test
    public void testDeleteIntDynamicConfig() throws PulsarAdminException {
        // Record the default value;
        int defaultValue = pulsar.getConfig().getMaxConcurrentTopicLoadRequest();
        // Set dynamic config.
        int newValue = defaultValue + 1000;
        admin.brokers().updateDynamicConfiguration("maxConcurrentTopicLoadRequest", newValue + "");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsar.getConfig().getMaxConcurrentTopicLoadRequest(), newValue);
        });
        // Verify: it has been reverted to the default value.
        admin.brokers().deleteDynamicConfiguration("maxConcurrentTopicLoadRequest");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsar.getConfig().getMaxConcurrentTopicLoadRequest(), defaultValue);
        });
    }

    @Test
    public void testDeleteCustomizedDynamicConfig() throws PulsarAdminException {
        // Record the default value;
        String customizedConfigName = "a123";
        pulsar.getBrokerService().registerCustomDynamicConfiguration(customizedConfigName, v -> true);

        AtomicReference<Object> currentValue = new AtomicReference<>();
        pulsar.getBrokerService().registerConfigurationListener(customizedConfigName, v -> {
            currentValue.set(v);
        });

        // The default value is null;
        Awaitility.await().untilAsserted(() -> {
            assertNull(currentValue.get());
        });

        // Set dynamic config.
        admin.brokers().updateDynamicConfiguration(customizedConfigName, "xxx");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(currentValue.get(), "xxx");
        });

        // Remove dynamic config.
        admin.brokers().deleteDynamicConfiguration(customizedConfigName);
        Awaitility.await().untilAsserted(() -> {
            assertNull(currentValue.get());
        });
    }
}
