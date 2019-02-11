package org.apache.pulsar.io.flume.sink;

import com.google.common.collect.Maps;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.AvroSink;
import org.apache.flume.source.AvroSource;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.flume.AbstractFlumeTests;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.junit.Assert;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

public class StringSinkTests extends AbstractFlumeTests {

    @Mock
    protected SinkContext mockSinkContext;

    @Mock
    protected Record<String> mockRecord;

    private AvroSource source;
    private AvroSink sink;
    private Channel channel;
    private InetAddress localhost;

    @BeforeMethod
    public void setUp() throws Exception {
        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);
        localhost = InetAddress.getByName("127.0.0.1");
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
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
                return Optional.of( "key-" + sequenceCounter++);
            }});

        when(mockRecord.getValue()).thenAnswer(new Answer<String>() {
            long sequenceCounter = 0;
            public String answer(InvocationOnMock invocation) throws Throwable {
                return new String( "value-" + sequenceCounter++);
            }});
    }

    @AfterMethod
    public void tearDown() throws Exception {

    }

    protected final void send(StringSink stringSink, int numRecords) throws Exception {
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

        Thread.sleep(3 * 1000); // Run for N seconds
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        Event event = channel.take();

        Assert.assertNotNull(event);
        Assert.assertNotNull(mockRecord);

        verify(mockRecord, times(100)).ack();
        transaction.commit();
        transaction.close();
        source.stop();
    }
}
