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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Properties;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.FieldParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class InProgressRequestFrameMemoryLimitModeTest {
    @DataProvider
    public Object[][] modes() {
        return new Object[][] {
            { "DISABLED", InProgressRequestFrameMemoryLimitMode.DISABLED },
            { "AUTO", InProgressRequestFrameMemoryLimitMode.AUTO },
            { "ENABLED", InProgressRequestFrameMemoryLimitMode.ENABLED },
            { "false", InProgressRequestFrameMemoryLimitMode.AUTO },
            { "true", InProgressRequestFrameMemoryLimitMode.ENABLED }
        };
    }

    @Test(dataProvider = "modes")
    public void shouldParseStartupAndDynamicValues(String value, InProgressRequestFrameMemoryLimitMode mode)
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty("inProgressRequestFrameMemoryLimitEnabled", value);
        ServiceConfiguration configuration = PulsarConfigurationLoader.create(properties, ServiceConfiguration.class);
        assertThat(configuration.getInProgressRequestFrameMemoryLimitEnabled()).isEqualTo(mode);
        assertThat(FieldParser.convert(value, InProgressRequestFrameMemoryLimitMode.class)).isEqualTo(mode);
    }

    @Test
    public void shouldDefaultToAutomaticProtectionAndRejectInvalidModes() {
        assertThat(new ServiceConfiguration().getInProgressRequestFrameMemoryLimitEnabled())
                .isEqualTo(InProgressRequestFrameMemoryLimitMode.AUTO);
        Properties properties = new Properties();
        properties.setProperty("inProgressRequestFrameMemoryLimitEnabled", "invalid");
        assertThatThrownBy(() -> PulsarConfigurationLoader.create(properties, ServiceConfiguration.class))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FieldParser.convert("invalid", InProgressRequestFrameMemoryLimitMode.class))
                .isInstanceOf(RuntimeException.class);
    }
}
