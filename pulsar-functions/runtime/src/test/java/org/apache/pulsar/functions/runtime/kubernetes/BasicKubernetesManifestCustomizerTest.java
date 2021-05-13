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
package org.apache.pulsar.functions.runtime.kubernetes;

import com.google.gson.Gson;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Toleration;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

/**
 * Unit test of {@link BasicKubernetesManifestCustomizerTest}.
 */
public class BasicKubernetesManifestCustomizerTest {

    @Test
    public void TestInitializeWithNullData() {
        BasicKubernetesManifestCustomizer customizer = new BasicKubernetesManifestCustomizer();
        customizer.initialize(null);
        assertNotEquals(customizer.getRuntimeOpts(), null);
        assertNull(customizer.getRuntimeOpts().getExtraLabels());
        assertNull(customizer.getRuntimeOpts().getExtraAnnotations());
        assertNull(customizer.getRuntimeOpts().getNodeSelectorLabels());
        assertNull(customizer.getRuntimeOpts().getTolerations());
        assertNull(customizer.getRuntimeOpts().getResourceRequirements());
    }

    @Test
    public void TestInitializeWithData() {
        BasicKubernetesManifestCustomizer customizer = new BasicKubernetesManifestCustomizer();
        Map<String, Object> confs = new HashMap<>();
        confs.put("jobNamespace", "custom-ns");
        confs.put("jobName", "custom-name");
        customizer.initialize(confs);
        assertNotEquals(customizer.getRuntimeOpts(), null);
        assertEquals(customizer.getRuntimeOpts().getJobName(), "custom-name");
        assertEquals(customizer.getRuntimeOpts().getJobNamespace(), "custom-ns");
    }

    @Test
    public void TestMergeRuntimeOpts() {
        Map<String, Object> configs = new Gson().fromJson(KubernetesRuntimeTest.createRuntimeCustomizerConfig(), HashMap.class);
        BasicKubernetesManifestCustomizer customizer = new BasicKubernetesManifestCustomizer();
        customizer.initialize(configs);
        BasicKubernetesManifestCustomizer.RuntimeOpts newOpts = new BasicKubernetesManifestCustomizer.RuntimeOpts();
        newOpts.setJobName("merged-name");
        newOpts.setTolerations(Collections.emptyList());
        V1Toleration toleration = new V1Toleration();
        toleration.setKey("merge-key");
        toleration.setEffect("NoSchedule");
        toleration.setOperator("Equal");
        toleration.setTolerationSeconds(6000L);
        newOpts.setTolerations(Collections.singletonList(toleration));
        V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
        resourceRequirements.putLimitsItem("cpu", new Quantity("20"));
        resourceRequirements.putLimitsItem("memory", new Quantity("10240"));
        newOpts.setResourceRequirements(resourceRequirements);
        newOpts.setNodeSelectorLabels(Collections.singletonMap("disktype", "ssd"));
        newOpts.setExtraAnnotations(Collections.singletonMap("functiontype", "sink"));
        newOpts.setExtraLabels(Collections.singletonMap("functiontype", "sink"));
        BasicKubernetesManifestCustomizer.RuntimeOpts mergedOpts = BasicKubernetesManifestCustomizer.mergeRuntimeOpts(
                customizer.getRuntimeOpts(), newOpts);

        assertEquals(mergedOpts.getJobName(), "merged-name");
        assertEquals(mergedOpts.getTolerations().size(), 2);
        assertEquals(mergedOpts.getExtraAnnotations().size(), 2);
        assertEquals(mergedOpts.getExtraLabels().size(), 2);
        assertEquals(mergedOpts.getNodeSelectorLabels().size(), 2);
        assertEquals(mergedOpts.getResourceRequirements().getLimits().get("cpu").getNumber().intValue(), 20);
        assertEquals(mergedOpts.getResourceRequirements().getLimits().get("memory").getNumber().intValue(), 10240);
    }
}
