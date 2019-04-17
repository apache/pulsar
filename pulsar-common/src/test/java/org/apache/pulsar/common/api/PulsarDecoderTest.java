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
package org.apache.pulsar.common.api;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandActiveConsumerChange;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link PulsarDecoder}.
 */
public class PulsarDecoderTest {

    private PulsarDecoder decoder;

    @BeforeMethod
    public void setup() {
        this.decoder = mock(PulsarDecoder.class, CALLS_REAL_METHODS);
    }

    @Test
    public void testChannelRead() throws Exception {
        long consumerId = 1234L;
        ByteBuf changeBuf = Commands.newActiveConsumerChange(consumerId, true);
        ByteBuf cmdBuf = changeBuf.slice(4, changeBuf.writerIndex() - 4);

        doNothing().when(decoder).handleActiveConsumerChange(any(CommandActiveConsumerChange.class));
        decoder.channelRead(mock(ChannelHandlerContext.class), cmdBuf);

        verify(decoder, times(1))
            .handleActiveConsumerChange(any(CommandActiveConsumerChange.class));
    }

    @Test
    public void testMaxSize() {
        String commonConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/common.conf";
        try {
            int maxMessageSize  = Integer.valueOf(loadConfig(commonConfigFile).getProperty("maxMessageSize"));
            assertEquals(maxMessageSize, PulsarDecoder.MaxMessageSize);
            int maxFrameSize = Integer.valueOf(loadConfig(commonConfigFile).getProperty("maxFrameSize"));
            assertEquals(maxFrameSize, PulsarDecoder.MaxFrameSize);
        } catch (IOException e) {
            assertEquals((5 * 1024 * 1024 - (10 * 1024)), PulsarDecoder.MaxMessageSize);
            assertEquals(5 * 1024 * 1024, PulsarDecoder.MaxFrameSize);
        }
    }

    private static Properties loadConfig(String configFile) throws IOException {
        try(FileInputStream inStream = new FileInputStream(configFile)) {
            Properties properties = new Properties();
            properties.load(inStream);
            return properties;
        }
    }

}
