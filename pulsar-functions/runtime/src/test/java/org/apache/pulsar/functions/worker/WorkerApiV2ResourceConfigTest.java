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
package org.apache.pulsar.functions.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.Locale;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.functions.auth.KubernetesSecretsTokenAuthProvider;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link WorkerConfig}.
 */
public class WorkerApiV2ResourceConfigTest {

    private static final String TEST_NAME = "test-worker-config";

    @Test
    public void testGetterSetter() {
        WorkerConfig wc = new WorkerConfig();
        wc.setPulsarServiceUrl("pulsar://localhost:1234");
        wc.setPulsarFunctionsNamespace("sample/standalone/functions");
        wc.setFunctionMetadataTopicName(TEST_NAME + "-meta-topic");
        wc.setNumFunctionPackageReplicas(3);
        wc.setWorkerId(TEST_NAME + "-worker");
        wc.setWorkerPort(1234);

        assertEquals("pulsar://localhost:1234", wc.getPulsarServiceUrl());
        assertEquals(TEST_NAME + "-meta-topic", wc.getFunctionMetadataTopicName());
        assertEquals("sample/standalone/functions", wc.getPulsarFunctionsNamespace());
        assertEquals(3, wc.getNumFunctionPackageReplicas());
        assertEquals(TEST_NAME + "-worker", wc.getWorkerId());
        assertEquals(new Integer(1234), wc.getWorkerPort());
    }

    @Test
    public void testLoadWorkerConfig() throws Exception {
        URL yamlUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());

        assertEquals("pulsar://localhost:6650", wc.getPulsarServiceUrl());
        assertEquals("test-function-metadata-topic", wc.getFunctionMetadataTopicName());
        assertEquals(3, wc.getNumFunctionPackageReplicas());
        assertEquals("test-worker", wc.getWorkerId());
        assertEquals(new Integer(7654), wc.getWorkerPort());
        assertEquals(200, wc.getMaxPendingAsyncRequests());
    }

    @Test
    public void testFunctionAuthProviderDefaults() throws Exception {
        URL emptyUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig emptyWc = WorkerConfig.load(emptyUrl.toURI().getPath());
        assertNull(emptyWc.getFunctionAuthProviderClassName());

        URL newK8SUrl = getClass().getClassLoader().getResource("test_worker_k8s_config.yml");
        WorkerConfig newK8SWc = WorkerConfig.load(newK8SUrl.toURI().getPath());
        assertEquals(newK8SWc.getFunctionRuntimeFactoryClassName(), KubernetesRuntimeFactory.class.getName());
        assertEquals(newK8SWc.getFunctionAuthProviderClassName(), KubernetesSecretsTokenAuthProvider.class.getName());

        URL legacyK8SUrl = getClass().getClassLoader().getResource("test_worker_k8s_legacy_config.yml");
        WorkerConfig legacyK8SWc = WorkerConfig.load(legacyK8SUrl.toURI().getPath());
        assertEquals(legacyK8SWc.getFunctionAuthProviderClassName(), KubernetesSecretsTokenAuthProvider.class.getName());

        URL overrideK8SUrl = getClass().getClassLoader().getResource("test_worker_k8s_auth_override_config.yml");
        WorkerConfig overrideK8SWc = WorkerConfig.load(overrideK8SUrl.toURI().getPath());
        assertEquals(overrideK8SWc.getFunctionAuthProviderClassName(), "org.apache.my.overridden.auth");

        URL emptyOverrideUrl = getClass().getClassLoader().getResource("test_worker_auth_override_config.yml");
        WorkerConfig emptyOverrideWc = WorkerConfig.load(emptyOverrideUrl.toURI().getPath());
        assertEquals(emptyOverrideWc.getFunctionAuthProviderClassName(),"org.apache.my.overridden.auth");
    }

    @Test
    public void testLoadResourceRestrictionsConfig() throws Exception {
        URL emptyUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig emptyWc = WorkerConfig.load(emptyUrl.toURI().getPath());
        assertNull(emptyWc.getFunctionInstanceMinResources());
        assertNull(emptyWc.getFunctionInstanceMaxResources());
        assertNull(emptyWc.getFunctionInstanceResourceGranularities());
        assertFalse(emptyWc.isFunctionInstanceResourceChangeInLockStep());

        URL newK8SUrl = getClass().getClassLoader().getResource("test_worker_k8s_resource_config.yml");
        WorkerConfig newK8SWc = WorkerConfig.load(newK8SUrl.toURI().getPath());
        assertNotNull(newK8SWc.getFunctionInstanceMinResources());
        assertEquals(newK8SWc.getFunctionInstanceMinResources().getCpu(), 0.5, 0.001);
        assertEquals(newK8SWc.getFunctionInstanceMinResources().getRam().longValue(), 1073741824L);
        assertEquals(newK8SWc.getFunctionInstanceMinResources().getDisk().longValue(), 10737418240L);

        assertNotNull(newK8SWc.getFunctionInstanceMaxResources());
        assertEquals(newK8SWc.getFunctionInstanceMaxResources().getCpu(), 16.0, 0.001);
        assertEquals(newK8SWc.getFunctionInstanceMaxResources().getRam().longValue(), 17179869184L);
        assertEquals(newK8SWc.getFunctionInstanceMaxResources().getDisk().longValue(), 107374182400L);

        assertNotNull(newK8SWc.getFunctionInstanceResourceGranularities());
        assertEquals(newK8SWc.getFunctionInstanceResourceGranularities().getCpu(), 1.0, 0.001);
        assertEquals(newK8SWc.getFunctionInstanceResourceGranularities().getRam().longValue(), 1073741824L);
        assertEquals(newK8SWc.getFunctionInstanceResourceGranularities().getDisk().longValue(), 10737418240L);

        assertTrue(newK8SWc.isFunctionInstanceResourceChangeInLockStep());
    }

    @Test
    public void testPasswordsNotLeakedOnToString() throws Exception {
        URL yamlUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
        assertFalse(wc.toString().toLowerCase(Locale.ROOT).contains("password"), "Stringified config must not contain password");
    }

    @Test
    public void testPasswordsPresentOnObjectMapping() throws Exception {
        URL yamlUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
        assertTrue((new ObjectMapper().writeValueAsString(wc)).toLowerCase(Locale.ROOT).contains("password"),
                "ObjectMapper output must include passwords for proper serialization");
    }
}
