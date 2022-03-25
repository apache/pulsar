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

package org.apache.pulsar.functions.secretsprovider;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.powermock.reflect.Whitebox;
import org.testng.annotations.Test;

public class EnvironmentBasedSecretsProviderTest {
    @Test
    public void testConfigValidation() throws Exception {
        EnvironmentBasedSecretsProvider provider = new EnvironmentBasedSecretsProvider();
        assertNull(provider.provideSecret("mySecretName", "Ignored"));
        injectEnvironmentVariable("mySecretName", "SecretValue");
        assertEquals(provider.provideSecret("mySecretName", "Ignored"), "SecretValue");
    }

    private static void injectEnvironmentVariable(String key, String value)
            throws Exception {

        Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
        Map<String,String> unmodifiableMap = new HashMap<>(Whitebox
                .getInternalState(processEnvironment, "theUnmodifiableEnvironment"));
        unmodifiableMap.put(key, value);
        Whitebox.setInternalState(processEnvironment, "theUnmodifiableEnvironment", unmodifiableMap);

        Map<String,String> envMap = new HashMap<>(Whitebox
                .getInternalState(processEnvironment, "theEnvironment"));
        envMap.put(key, value);
        Whitebox.setInternalState(processEnvironment, "theEnvironment", envMap);
    }
}
