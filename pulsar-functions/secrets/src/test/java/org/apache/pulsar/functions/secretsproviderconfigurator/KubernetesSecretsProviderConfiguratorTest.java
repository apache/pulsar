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

package org.apache.pulsar.functions.secretsproviderconfigurator;

import com.google.gson.Gson;
import org.apache.pulsar.functions.proto.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;

/**
 * Unit test of {@link KubernetesSecretsProviderConfigurator}.
 */
public class KubernetesSecretsProviderConfiguratorTest {

    @Test
    public void testConfigValidation() throws Exception {
        KubernetesSecretsProviderConfigurator provider = new KubernetesSecretsProviderConfigurator();
        try {
            HashMap<String, Object> map = new HashMap<String, Object>();
            map.put("secretname", "randomsecret");
            Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder().setSecretsMap(new Gson().toJson(map)).build();
            provider.doAdmissionChecks(null, null, null, null, functionDetails);
            Assert.fail("Non conforming secret object should not validate");
        } catch (Exception e) {
        }
        try {
            HashMap<String, Object> map = new HashMap<String, Object>();
            HashMap<String, String> map1 = new HashMap<String, String>();
            map1.put("secretname", "secretvalue");
            map.put("secretname", map1);
            Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder().setSecretsMap(new Gson().toJson(map)).build();
            provider.doAdmissionChecks(null, null, null, null, functionDetails);
            Assert.fail("Non conforming secret object should not validate");
        } catch (Exception e) {
        }
        try {
            HashMap<String, Object> map = new HashMap<String, Object>();
            HashMap<String, String> map1 = new HashMap<String, String>();
            map1.put("path", "secretvalue");
            map1.put("key", "secretvalue");
            map.put("secretname", map1);
            Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder().setSecretsMap(new Gson().toJson(map)).build();
            provider.doAdmissionChecks(null, null, null, null, functionDetails);
        } catch (Exception e) {
            Assert.fail("Conforming secret object should validate");
        }
    }
}
