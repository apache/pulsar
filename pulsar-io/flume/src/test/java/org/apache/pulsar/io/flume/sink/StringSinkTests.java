package org.apache.pulsar.io.flume.sink;

import com.google.common.collect.Maps;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.sink.AvroSink;
import org.apache.flume.source.AvroSource;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.junit.Assert;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StringSinkTests {

    @Mock
    protected SinkContext mockSinkContext;

    private AvroSource source;
    private AvroSink sink;
    private Channel channel;
    private InetAddress localhost;

    @BeforeMethod
    public void setUp() throws Exception {
        localhost = InetAddress.getByName("127.0.0.1");
        source = new AvroSource();
        sink = new AvroSink();
        channel = new MemoryChannel();
        Context context = new Context();
        context.put("port", String.valueOf(44444));
        context.put("bind", "0.0.0.0");

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
    }

    @AfterMethod
    public void tearDown() throws Exception {

    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        Map<String, Object> conf = Maps.newHashMap();
        StringSink stringSink = new StringSink();
        conf.put("name", "a1");
        conf.put("confFile", "flume-io-source.yaml");
        conf.put("noReloadConf", false);
        conf.put("zkConnString", "");
        conf.put("zkBasePath", "");
        stringSink.open(conf, null);
//        stringSink.write();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        Event event = channel.take();
        Assert.assertNotNull(event);
        Assert.assertEquals("Channel contained our event", "Hello avro",
                new String(event.getBody()));
        transaction.commit();
        transaction.close();
        source.stop();
    }
}
