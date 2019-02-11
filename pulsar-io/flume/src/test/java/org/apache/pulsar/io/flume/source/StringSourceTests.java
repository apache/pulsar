package org.apache.pulsar.io.flume.source;

import org.apache.flume.conf.Configurables;
import org.testng.annotations.BeforeMethod;
import org.apache.flume.Context;
import org.apache.flume.Channel;
import org.apache.flume.sink.AvroSink;
import org.apache.flume.channel.MemoryChannel;

import org.apache.pulsar.io.flume.AbstractFlumeTests;

public class StringSourceTests extends AbstractFlumeTests {

    private AvroSink sink;
    private Channel channel;

    @BeforeMethod
    public void setUp() throws Exception {
        if (sink != null) {
            throw new RuntimeException("double setup");
        }
        Context context = new Context();
        context.put("hostname", "127.0.0.1");
        context.put("port", "44444");
        context.put("batch-size", String.valueOf(2));
        context.put("connect-timeout", String.valueOf(2000L));
        context.put("request-timeout", String.valueOf(3000L));
        sink = new AvroSink();
        channel = new MemoryChannel();
        sink.setChannel(channel);
        Configurables.configure(sink, context);
        Configurables.configure(channel, context);
    }
}
