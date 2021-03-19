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
package org.apache.pulsar.io.flume.source;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.AvroSink;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.flume.AbstractFlumeTests;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StringSourceTests extends AbstractFlumeTests {

    private AvroSink sink;

    private Channel channel;

    @Mock
    private SourceContext mockSourceContext;

    @BeforeMethod
    public void setUp() throws Exception {
        if (sink != null) {
            throw new RuntimeException("double setup");
        }
        Context context = new Context();
        context.put("hostname", "127.0.0.1");
        context.put("port", "44445");
        context.put("batch-size", String.valueOf(2));
        context.put("connect-timeout", String.valueOf(2000L));
        context.put("request-timeout", String.valueOf(3000L));
        sink = new AvroSink();
        channel = new MemoryChannel();
        sink.setChannel(channel);
        Configurables.configure(sink, context);
        Configurables.configure(channel, context);

        mockSourceContext = mock(SourceContext.class);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        sink.stop();
        sink = null;
    }

    @Test
    public void TestOpenAndReadSource() throws Exception {
        Map<String, Object> conf = Maps.newHashMap();
        StringSource stringSource = new StringSource();
        conf.put("name", "a1");
        conf.put("confFile", "./src/test/resources/flume/sink.conf");
        conf.put("noReloadConf", false);
        conf.put("zkConnString", "");
        conf.put("zkBasePath", "");
        Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
        stringSource.open(conf, mockSourceContext);
        Thread.sleep(3 * 1000);
        sink.start();
        Transaction transaction = channel.getTransaction();

        transaction.begin();
        for (int i = 0; i < 10; i++) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();

        for (int i = 0; i < 5; i++) {
            Sink.Status status = sink.process();
            assertEquals(status, Sink.Status.READY);
        }

        assertEquals(sink.process(), Sink.Status.BACKOFF);
        stringSource.close();
    }
}
