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
package org.apache.pulsar.io.flume.sink;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.flume.AbstractFlumeTests;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StringSinkTests extends AbstractFlumeTests {

    @Mock
    protected SinkContext mockSinkContext;

    @Mock
    protected Record<String> mockRecord;


    private AvroSource source;
    private Channel channel;

    @BeforeMethod
    public void setUp() {
        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);
        source = new AvroSource();
        channel = new MemoryChannel();
        Context context = new Context();
        context.put("port", String.valueOf(44444));
        context.put("bind", "0.0.0.0");

        Configurables.configure(source, context);
        Configurables.configure(channel, context);

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));

        source.start();

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            long sequenceCounter = 0;

            public Optional<String> answer(InvocationOnMock invocation) {
                return Optional.of("key-" + sequenceCounter++);
            }
        });

        when(mockRecord.getValue()).thenAnswer(new Answer<String>() {
            long sequenceCounter = 0;

            public String answer(InvocationOnMock invocation) {
                return new String("value-" + sequenceCounter++);
            }
        });
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        source.stop();
    }

    protected final void send(StringSink stringSink, int numRecords) {
        for (int idx = 0; idx < numRecords; idx++) {
            stringSink.write(mockRecord);
        }
    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        Map<String, Object> conf = Maps.newHashMap();
        StringSink stringSink = new StringSink();
        conf.put("name", "a1");
        conf.put("confFile", "./src/test/resources/flume/source.conf");
        conf.put("noReloadConf", false);
        conf.put("zkConnString", "");
        conf.put("zkBasePath", "");
        stringSink.open(conf, mockSinkContext);
        send(stringSink, 100);

        Thread.sleep(3 * 1000);
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        Event event = channel.take();

        Assert.assertNotNull(event);
        Assert.assertNotNull(mockRecord);

        verify(mockRecord, times(100)).ack();
        transaction.commit();
        transaction.close();
    }
}
