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
package org.apache.pulsar.io.common;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class IOConfigUtilsTest {

    @Data
    static class TestConfig {
        @FieldDoc(
                required = true,
                defaultValue = "",
                sensitive = true,
                help = "password"
        )
        private String password;

        @FieldDoc(
                required = true,
                defaultValue = "",
                sensitive = false,
                help = ""
        )
        private String notSensitive;

        /**
         * Non-string secrets are not supported at this moment
         */
        @FieldDoc(
                required = true,
                defaultValue = "",
                sensitive = true,
                help = ""
        )
        private long sensitiveLong;
    }

    static class TestSourceContext implements SourceContext {

        static Map<String, String> secretsMap = new HashMap<>();
        static {
            secretsMap.put("password", "my-password");
        }

        @Override
        public int getInstanceId() {
            return 0;
        }

        @Override
        public int getNumInstances() {
            return 0;
        }

        @Override
        public void recordMetric(String metricName, double value) {

        }

        @Override
        public String getOutputTopic() {
            return null;
        }

        @Override
        public String getTenant() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public String getSourceName() {
            return null;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public String getSecret(String secretName) {
            return secretsMap.get(secretName);
        }
    }

    @Test
    public void loadWithSecrets() {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        TestConfig testConfig = IOConfigUtils.loadWithSecrets(configMap, TestConfig.class, new TestSourceContext());

        Assert.assertEquals(testConfig.notSensitive, "foo");
        Assert.assertEquals(testConfig.password, "my-password");

        configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        configMap.put("password", "another-password");
        configMap.put("sensitiveLong", 5L);

        testConfig = IOConfigUtils.loadWithSecrets(configMap, TestConfig.class, new TestSourceContext());

        Assert.assertEquals(testConfig.notSensitive, "foo");
        Assert.assertEquals(testConfig.password, "my-password");
        Assert.assertEquals(testConfig.sensitiveLong, 5L);
    }
}
