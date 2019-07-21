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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

public class ClientCnxTest {

    @Test
    public void testClientCnxTimeout() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        when(listenerFuture.addListener(any())).thenReturn(listenerFuture);
        when(ctx.writeAndFlush(any())).thenReturn(listenerFuture);

        Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
        ctxField.setAccessible(true);
        ctxField.set(cnx, ctx);
        try {
            cnx.newLookup(null, 123).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }
    }

}
