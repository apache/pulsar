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
package org.apache.pulsar.common.configuration;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;
import java.util.Properties;

public class PulsarConfigurationUtilsTest {
    @Test
    public void testLoadArbitraryBookieConfiguration() {
        ClientConfiguration bkConf = new ClientConfiguration();
        Properties props = new Properties();
        // Use a config that will only get applied with the correct prefix and that
        props.setProperty("bookkeeper_addEntryTimeoutSec", "200");
        props.setProperty("addEntryTimeoutSec", "100");

        Assert.assertNotEquals(bkConf.getAddEntryTimeout(), 200, "Should get the BK default");

        // Submit conf to get updated
        PulsarConfigurationUtils.loadPrefixedBookieClientConfiguration(bkConf, props, "test");

        Assert.assertEquals(bkConf.getAddEntryTimeout(), 200, "Only the prefixed value should apply");
    }
    @Test
    public void testLoadArbitraryBookieConfigurationDLog() {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        Properties props = new Properties();
        // Use a config that will only get applied with the correct prefix and that
        props.setProperty("bookkeeper_addEntryTimeoutSec", "200");
        props.setProperty("addEntryTimeoutSec", "100");

        // Assert none of the configs are set yet.
        Assert.assertThrows(NoSuchElementException.class, () -> conf.getInt("addEntryTimeoutSec"));
        Assert.assertThrows(NoSuchElementException.class, () -> conf.getInt("bkc.addEntryTimeoutSec"));

        // Submit conf to get updated
        PulsarConfigurationUtils.loadPrefixedBookieClientConfiguration(conf, props, "test");

        // Ensure the configuration is set using the proper distributed log prefix for bookkeeper configs.
        Assert.assertThrows(NoSuchElementException.class, () -> conf.getInt("addEntryTimeoutSec"));
        Assert.assertEquals(conf.getInt("bkc.addEntryTimeoutSec"), 200,
                "Only the prefixed value should apply");
    }
}
