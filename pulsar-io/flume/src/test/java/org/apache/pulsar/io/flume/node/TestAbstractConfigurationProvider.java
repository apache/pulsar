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
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.channel.AbstractChannel;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.source.AbstractSource;
import org.testng.annotations.Test;

public class TestAbstractConfigurationProvider {

    @Test
    public void testDispoableChannel() throws Exception {
        String agentName = "agent1";
        Map<String, String> properties = getPropertiesForChannel(agentName,
                DisposableChannel.class.getName());
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);
        MaterializedConfiguration config1 = provider.getConfiguration();
        Channel channel1 = config1.getChannels().values().iterator().next();
        assertTrue(channel1 instanceof DisposableChannel);
        MaterializedConfiguration config2 = provider.getConfiguration();
        Channel channel2 = config2.getChannels().values().iterator().next();
        assertTrue(channel2 instanceof DisposableChannel);
        assertNotSame(channel1, channel2);
    }

    @Test
    public void testReusableChannel() throws Exception {
        String agentName = "agent1";
        Map<String, String> properties = getPropertiesForChannel(agentName,
                RecyclableChannel.class.getName());
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);

        MaterializedConfiguration config1 = provider.getConfiguration();
        Channel channel1 = config1.getChannels().values().iterator().next();
        assertTrue(channel1 instanceof RecyclableChannel);

        MaterializedConfiguration config2 = provider.getConfiguration();
        Channel channel2 = config2.getChannels().values().iterator().next();
        assertTrue(channel2 instanceof RecyclableChannel);

        assertSame(channel1, channel2);
    }

    @Test
    public void testUnspecifiedChannel() throws Exception {
        String agentName = "agent1";
        Map<String, String> properties = getPropertiesForChannel(agentName,
                UnspecifiedChannel.class.getName());
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);

        MaterializedConfiguration config1 = provider.getConfiguration();
        Channel channel1 = config1.getChannels().values().iterator().next();
        assertTrue(channel1 instanceof UnspecifiedChannel);

        MaterializedConfiguration config2 = provider.getConfiguration();
        Channel channel2 = config2.getChannels().values().iterator().next();
        assertTrue(channel2 instanceof UnspecifiedChannel);

        assertSame(channel1, channel2);
    }

    @Test
    public void testReusableChannelNotReusedLater() throws Exception {
        String agentName = "agent1";
        Map<String, String> propertiesReusable = getPropertiesForChannel(agentName,
                RecyclableChannel.class
                        .getName());
        Map<String, String> propertiesDispoable = getPropertiesForChannel(agentName,
                DisposableChannel.class
                        .getName());
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, propertiesReusable);
        MaterializedConfiguration config1 = provider.getConfiguration();
        Channel channel1 = config1.getChannels().values().iterator().next();
        assertTrue(channel1 instanceof RecyclableChannel);

        provider.setProperties(propertiesDispoable);
        MaterializedConfiguration config2 = provider.getConfiguration();
        Channel channel2 = config2.getChannels().values().iterator().next();
        assertTrue(channel2 instanceof DisposableChannel);

        provider.setProperties(propertiesReusable);
        MaterializedConfiguration config3 = provider.getConfiguration();
        Channel channel3 = config3.getChannels().values().iterator().next();
        assertTrue(channel3 instanceof RecyclableChannel);

        assertNotSame(channel1, channel3);
    }

    @Test
    public void testSourceThrowsExceptionDuringConfiguration() throws Exception {
        String agentName = "agent1";
        String sourceType = UnconfigurableSource.class.getName();
        String channelType = "memory";
        String sinkType = "null";
        Map<String, String> properties = getProperties(agentName, sourceType,
                channelType, sinkType);
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);
        MaterializedConfiguration config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 0);
        assertEquals(config.getChannels().size(), 1);
        assertEquals(config.getSinkRunners().size(), 1);
    }

    @Test
    public void testChannelThrowsExceptionDuringConfiguration() throws Exception {
        String agentName = "agent1";
        String sourceType = "seq";
        String channelType = UnconfigurableChannel.class.getName();
        String sinkType = "null";
        Map<String, String> properties = getProperties(agentName, sourceType,
                channelType, sinkType);
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);
        MaterializedConfiguration config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 0);
        assertEquals(config.getChannels().size(), 0);
        assertEquals(config.getSinkRunners().size(), 0);
    }

    @Test
    public void testSinkThrowsExceptionDuringConfiguration() throws Exception {
        String agentName = "agent1";
        String sourceType = "seq";
        String channelType = "memory";
        String sinkType = UnconfigurableSink.class.getName();
        Map<String, String> properties = getProperties(agentName, sourceType,
                channelType, sinkType);
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);
        MaterializedConfiguration config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 1);
        assertEquals(config.getChannels().size(), 1);
        assertEquals(config.getSinkRunners().size(), 0);
    }

    @Test
    public void testSourceAndSinkThrowExceptionDuringConfiguration()
            throws Exception {
        String agentName = "agent1";
        String sourceType = UnconfigurableSource.class.getName();
        String channelType = "memory";
        String sinkType = UnconfigurableSink.class.getName();
        Map<String, String> properties = getProperties(agentName, sourceType,
                channelType, sinkType);
        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);
        MaterializedConfiguration config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 0);
        assertEquals(config.getChannels().size(), 0);
        assertEquals(config.getSinkRunners().size(), 0);
    }

    @Test
    public void testSinkSourceMismatchDuringConfiguration() throws Exception {
        String agentName = "agent1";
        String sourceType = "seq";
        String channelType = "memory";
        String sinkType = "avro";
        Map<String, String> properties = getProperties(agentName, sourceType,
                channelType, sinkType);
        properties.put(agentName + ".channels.channel1.capacity", "1000");
        properties.put(agentName + ".channels.channel1.transactionCapacity", "1000");
        properties.put(agentName + ".sources.source1.batchSize", "1000");
        properties.put(agentName + ".sinks.sink1.batch-size", "1000");
        properties.put(agentName + ".sinks.sink1.hostname", "10.10.10.10");
        properties.put(agentName + ".sinks.sink1.port", "1010");

        MemoryConfigurationProvider provider =
                new MemoryConfigurationProvider(agentName, properties);
        MaterializedConfiguration config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 1);
        assertEquals(config.getChannels().size(), 1);
        assertEquals(config.getSinkRunners().size(), 1);

        properties.put(agentName + ".sources.source1.batchSize", "1001");
        properties.put(agentName + ".sinks.sink1.batch-size", "1000");

        provider = new MemoryConfigurationProvider(agentName, properties);
        config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 0);
        assertEquals(config.getChannels().size(), 1);
        assertEquals(config.getSinkRunners().size(), 1);

        properties.put(agentName + ".sources.source1.batchSize", "1000");
        properties.put(agentName + ".sinks.sink1.batch-size", "1001");

        provider = new MemoryConfigurationProvider(agentName, properties);
        config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 1);
        assertEquals(config.getChannels().size(), 1);
        assertEquals(config.getSinkRunners().size(), 0);

        properties.put(agentName + ".sources.source1.batchSize", "1001");
        properties.put(agentName + ".sinks.sink1.batch-size", "1001");

        provider = new MemoryConfigurationProvider(agentName, properties);
        config = provider.getConfiguration();
        assertEquals(config.getSourceRunners().size(), 0);
        assertEquals(config.getChannels().size(), 0);
        assertEquals(config.getSinkRunners().size(), 0);
    }

    private Map<String, String> getProperties(String agentName,
                                              String sourceType, String channelType,
                                              String sinkType) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(agentName + ".sources", "source1");
        properties.put(agentName + ".channels", "channel1");
        properties.put(agentName + ".sinks", "sink1");
        properties.put(agentName + ".sources.source1.type", sourceType);
        properties.put(agentName + ".sources.source1.channels", "channel1");
        properties.put(agentName + ".channels.channel1.type", channelType);
        properties.put(agentName + ".channels.channel1.capacity", "100");
        properties.put(agentName + ".sinks.sink1.type", sinkType);
        properties.put(agentName + ".sinks.sink1.channel", "channel1");
        return properties;
    }

    private Map<String, String> getPropertiesForChannel(String agentName, String channelType) {
        return getProperties(agentName, "seq", channelType, "null");
    }

    public static class MemoryConfigurationProvider extends AbstractConfigurationProvider {
        private Map<String, String> properties;

        public MemoryConfigurationProvider(String agentName, Map<String, String> properties) {
            super(agentName);
            this.properties = properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        @Override
        protected FlumeConfiguration getFlumeConfiguration() {
            return new FlumeConfiguration(properties);
        }
    }

    @Disposable
    public static class DisposableChannel extends AbstractChannel {
        @Override
        public void put(Event event) throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Event take() throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Transaction getTransaction() {
            throw new UnsupportedOperationException();
        }
    }

    @Recyclable
    public static class RecyclableChannel extends AbstractChannel {
        @Override
        public void put(Event event) throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Event take() throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Transaction getTransaction() {
            throw new UnsupportedOperationException();
        }
    }

    public static class UnspecifiedChannel extends AbstractChannel {
        @Override
        public void put(Event event) throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Event take() throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Transaction getTransaction() {
            throw new UnsupportedOperationException();
        }
    }

    public static class UnconfigurableChannel extends AbstractChannel {
        @Override
        public void configure(Context context) {
            throw new RuntimeException("expected");
        }

        @Override
        public void put(Event event) throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Event take() throws ChannelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Transaction getTransaction() {
            throw new UnsupportedOperationException();
        }
    }

    public static class UnconfigurableSource extends AbstractSource implements Configurable {
        @Override
        public void configure(Context context) {
            throw new RuntimeException("expected");
        }
    }

    public static class UnconfigurableSink extends AbstractSink implements Configurable {
        @Override
        public void configure(Context context) {
            throw new RuntimeException("expected");
        }

        @Override
        public Status process() throws EventDeliveryException {
            throw new UnsupportedOperationException();
        }
    }
}
