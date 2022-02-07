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
package org.apache.pulsar.common.policies.data;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class EnsemblePlacementPolicyConfigTest {

    static class MockedEnsemblePlacementPolicy {}

    @Test
    public void testEncodeDecodeSuccessfully()
        throws EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException {

        EnsemblePlacementPolicyConfig originalConfig =
            new EnsemblePlacementPolicyConfig(MockedEnsemblePlacementPolicy.class, Collections.emptyMap());
        byte[] encodedConfig = originalConfig.encode();

        EnsemblePlacementPolicyConfig decodedConfig =
            EnsemblePlacementPolicyConfig.decode(encodedConfig);
        Assert.assertEquals(decodedConfig, originalConfig);
    }

    @Test
    public void testDecodeFailed() {
        byte[] configBytes = new byte[0];
        try {
            EnsemblePlacementPolicyConfig.decode(configBytes);
            Assert.fail("should failed parse the config from bytes");
        } catch (EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException e) {
            // expected error
        }
    }
}
