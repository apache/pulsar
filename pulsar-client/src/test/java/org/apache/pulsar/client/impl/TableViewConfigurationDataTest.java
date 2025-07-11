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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.testng.annotations.Test;

/**
 * Unit test of {@link TableViewConfigurationData}.
 */
public class TableViewConfigurationDataTest {
    @Test
    public void testLoadConfigFromMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("topicName", "persistent://public/default/test");
        configMap.put("subscriptionName", "test-sub");
        configMap.put("autoUpdatePartitionsSeconds", "60");
        TableViewConfigurationData config = new TableViewConfigurationData();
        config = ConfigurationDataUtils.loadData(
                configMap, config, TableViewConfigurationData.class);

        assertEquals(configMap.get("topicName"), config.getTopicName());
        assertEquals(configMap.get("subscriptionName"), config.getSubscriptionName());
        assertEquals(configMap.get("autoUpdatePartitionsSeconds"), String.valueOf(config.getAutoUpdatePartitionsSeconds()));
    }
}
