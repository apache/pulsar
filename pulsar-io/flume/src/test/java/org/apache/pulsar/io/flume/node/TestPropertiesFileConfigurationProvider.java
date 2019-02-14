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
package org.apache.pulsar.io.flume.node;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration;
import org.apache.flume.conf.FlumeConfigurationError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TestPropertiesFileConfigurationProvider {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(TestPropertiesFileConfigurationProvider.class);

    private static final File TESTFILE = new File(
            TestPropertiesFileConfigurationProvider.class.getClassLoader()
                    .getResource("flume-conf.properties").getFile());

    private PropertiesFileConfigurationProvider provider;

    @Before
    public void setUp() throws Exception {
        provider = new PropertiesFileConfigurationProvider("test", TESTFILE);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testPropertyRead() throws Exception {

        FlumeConfiguration configuration = provider.getFlumeConfiguration();
        Assert.assertNotNull(configuration);

    /*
     * Test the known errors in the file
     */
        List<String> expected = Lists.newArrayList();
        expected.add("host5 CONFIG_ERROR");
        expected.add("host5 INVALID_PROPERTY");
        expected.add("host4 CONFIG_ERROR");
        expected.add("host4 CONFIG_ERROR");
        expected.add("host4 PROPERTY_VALUE_NULL");
        expected.add("host4 PROPERTY_VALUE_NULL");
        expected.add("host4 PROPERTY_VALUE_NULL");
        expected.add("host4 AGENT_CONFIGURATION_INVALID");
        expected.add("ch2 ATTRS_MISSING");
        expected.add("host3 CONFIG_ERROR");
        expected.add("host3 PROPERTY_VALUE_NULL");
        expected.add("host3 AGENT_CONFIGURATION_INVALID");
        expected.add("host2 PROPERTY_VALUE_NULL");
        expected.add("host2 AGENT_CONFIGURATION_INVALID");
        List<String> actual = Lists.newArrayList();
        for (FlumeConfigurationError error : configuration.getConfigurationErrors()) {
            actual.add(error.getComponentName() + " " + error.getErrorType().toString());
        }
        Collections.sort(expected);
        Collections.sort(actual);
        Assert.assertEquals(expected, actual);

        AgentConfiguration agentConfiguration =
                configuration.getConfigurationFor("host1");
        Assert.assertNotNull(agentConfiguration);

        LOGGER.info(agentConfiguration.getPrevalidationConfig());
        LOGGER.info(agentConfiguration.getPostvalidationConfig());

        Set<String> sources = Sets.newHashSet("source1");
        Set<String> sinks = Sets.newHashSet("sink1");
        Set<String> channels = Sets.newHashSet("channel1");

        Assert.assertEquals(sources, agentConfiguration.getSourceSet());
        Assert.assertEquals(sinks, agentConfiguration.getSinkSet());
        Assert.assertEquals(channels, agentConfiguration.getChannelSet());
    }
}
