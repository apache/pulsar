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

import static org.testng.Assert.assertEquals;
import java.util.Properties;
import org.testng.annotations.Test;

public class ServiceConfigurationUtilsTest {
    @Test
    public void testGetLongPropertyOrDefault() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        Properties properties = new Properties();
        properties.put("test", 1);
        configuration.setProperties(properties);
        long value = ServiceConfigurationUtils.getLongPropertyOrDefault(configuration, "test", 999L);
        assertEquals(value, 1L);
        properties.put("test", 2L);
        value = ServiceConfigurationUtils.getLongPropertyOrDefault(configuration, "test", 999L);
        assertEquals(value, 2L);
        properties.put("test", "3");
        value = ServiceConfigurationUtils.getLongPropertyOrDefault(configuration, "test", 999L);
        assertEquals(value, 3L);
        properties.put("test", "");
        value = ServiceConfigurationUtils.getLongPropertyOrDefault(configuration, "test", 999L);
        assertEquals(value, 999L);
    }
}
