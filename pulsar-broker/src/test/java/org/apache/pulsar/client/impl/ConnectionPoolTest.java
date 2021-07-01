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
package org.apache.pulsar.client.impl;

import com.google.common.collect.Lists;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

@Test(groups = "broker-impl")
public class ConnectionPoolTest extends MockedPulsarServiceBaseTest {

    String serviceUrl;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        serviceUrl = "pulsar://non-existing-dns-name:" + pulsar.getBrokerListenPort().get();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSingleIpAddress() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("test"));
        ConnectionPool pool = Mockito.spy(new ConnectionPool(conf, eventLoop));
        conf.setServiceUrl(serviceUrl);
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoop, pool);

        List<InetAddress> result = Lists.newArrayList();
        result.add(InetAddress.getByName("127.0.0.1"));
        Mockito.when(pool.resolveName("non-existing-dns-name")).thenReturn(CompletableFuture.completedFuture(result));

        client.newProducer().topic("persistent://sample/standalone/ns/my-topic").create();

        client.close();
        eventLoop.shutdownGracefully();
    }

    @Test
    public void testDoubleIpAddress() throws Exception {
        String serviceUrl = "pulsar://non-existing-dns-name:" + pulsar.getBrokerListenPort().get();

        ClientConfigurationData conf = new ClientConfigurationData();
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("test"));
        ConnectionPool pool = Mockito.spy(new ConnectionPool(conf, eventLoop));
        conf.setServiceUrl(serviceUrl);
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoop, pool);

        List<InetAddress> result = Lists.newArrayList();

        // Add a non existent IP to the response to check that we're trying the 2nd address as well
        result.add(InetAddress.getByName("127.0.0.99"));
        result.add(InetAddress.getByName("127.0.0.1"));
        Mockito.when(pool.resolveName("non-existing-dns-name")).thenReturn(CompletableFuture.completedFuture(result));

        // Create producer should succeed by trying the 2nd IP
        client.newProducer().topic("persistent://sample/standalone/ns/my-topic").create();
        client.close();

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testNoConnectionPool() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConnectionsPerBroker(0);
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(8, false, new DefaultThreadFactory("test"));
        ConnectionPool pool = Mockito.spy(new ConnectionPool(conf, eventLoop));

        InetSocketAddress brokerAddress =
            InetSocketAddress.createUnresolved("127.0.0.1", pulsar.getBrokerListenPort().get());
        IntStream.range(1, 5).forEach(i -> {
            pool.getConnection(brokerAddress).thenAccept(cnx -> {
                Assert.assertTrue(cnx.channel().isActive());
                pool.releaseConnection(cnx);
                Assert.assertTrue(cnx.channel().isActive());
            });
        });
        Assert.assertEquals(pool.getPoolSize(), 0);

        pool.closeAllConnections();
        pool.close();
    }

    @Test
    public void testEnableConnectionPool() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConnectionsPerBroker(5);
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(8, false, new DefaultThreadFactory("test"));
        ConnectionPool pool = Mockito.spy(new ConnectionPool(conf, eventLoop));

        InetSocketAddress brokerAddress =
            InetSocketAddress.createUnresolved("127.0.0.1", pulsar.getBrokerListenPort().get());
        IntStream.range(1, 10).forEach(i -> {
            pool.getConnection(brokerAddress).thenAccept(cnx -> {
                Assert.assertTrue(cnx.channel().isActive());
                pool.releaseConnection(cnx);
                Assert.assertTrue(cnx.channel().isActive());
            });
        });
        Assert.assertTrue(pool.getPoolSize() <= 5 && pool.getPoolSize() > 0);

        pool.closeAllConnections();
        pool.close();
    }
}
