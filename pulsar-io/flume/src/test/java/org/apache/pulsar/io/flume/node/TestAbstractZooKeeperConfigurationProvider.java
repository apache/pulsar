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

import static org.testng.Assert.assertEquals;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.EnsurePath;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.conf.FlumeConfigurationError;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class TestAbstractZooKeeperConfigurationProvider {

    private static final String FLUME_CONF_FILE = "flume-conf.properties";

    protected static final String AGENT_NAME = "a1";

    protected static final String AGENT_PATH =
            AbstractZooKeeperConfigurationProvider.DEFAULT_ZK_BASE_PATH + "/" + AGENT_NAME;

    protected TestingServer zkServer;
    protected CuratorFramework client;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        // start the instance without the admin server!
        InstanceSpec serverSpec = new InstanceSpec(null, -1, -1, -1, true, -1, -1, -1, Collections.singletonMap("zookeeper.admin.enableServer", "false"));
        zkServer = new TestingServer(serverSpec, true);
        client = CuratorFrameworkFactory
                .newClient("localhost:" + zkServer.getPort(),
                        new ExponentialBackoffRetry(1000, 3));
        client.start();

        EnsurePath ensurePath = new EnsurePath(AGENT_PATH);
        ensurePath.ensure(client.getZookeeperClient());
        doSetUp();
    }

    protected abstract void doSetUp() throws Exception;

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        doTearDown();
        zkServer.close();
        client.close();
    }

    protected abstract void doTearDown() throws Exception;

    protected void addData() throws Exception {
        Reader in = new InputStreamReader(getClass().getClassLoader()
                .getResourceAsStream(FLUME_CONF_FILE), Charsets.UTF_8);
        try {
            String config = IOUtils.toString(in);
            client.setData().forPath(AGENT_PATH, config.getBytes());
        } finally {
            in.close();
        }
    }

    protected void verifyProperties(AbstractConfigurationProvider cp) {
        FlumeConfiguration configuration = cp.getFlumeConfiguration();
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
        assertEquals(actual, expected);

        FlumeConfiguration.AgentConfiguration agentConfiguration = configuration
                .getConfigurationFor("host1");
        Assert.assertNotNull(agentConfiguration);

        Set<String> sources = Sets.newHashSet("source1");
        Set<String> sinks = Sets.newHashSet("sink1");
        Set<String> channels = Sets.newHashSet("channel1");

        assertEquals(agentConfiguration.getSourceSet(), sources);
        assertEquals(agentConfiguration.getSinkSet(), sinks);
        assertEquals(agentConfiguration.getChannelSet(), channels);
    }
}
