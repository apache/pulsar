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
package org.apache.pulsar.common.util.netty;

import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoop;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Test {@link ChannelFutures}.
 */
public class ChannelFuturesTest {

    @Mock
    private Channel channel;

    private DefaultEventLoop eventLoop;

    private DefaultChannelPromise channelFuture;

    @BeforeTest
    public void initEventLoop() {
        eventLoop = new DefaultEventLoop();
    }

    @AfterTest(alwaysRun = true)
    public void shutdownEventLoop() throws InterruptedException {
        if (eventLoop != null) {
            eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).await(100);
        }
    }

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(channel.eventLoop()).thenReturn(eventLoop);

        channelFuture = new DefaultChannelPromise(channel);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void toCompletableFuture_shouldRequireNonNullArgument() {
        ChannelFutures.toCompletableFuture(null);
    }

    @Test
    public void toCompletableFuture_shouldCompleteSuccessfully_channelFutureCompletedBefore() throws Exception {
        channelFuture.setSuccess();
        Assert.assertEquals(ChannelFutures.toCompletableFuture(channelFuture).get(1,  TimeUnit.SECONDS), channel);
    }

    @Test
    public void toCompletableFuture_shouldCompleteSuccessfully_channelFutureCompletedAfter() throws Exception {
        CompletableFuture<Channel> future = ChannelFutures.toCompletableFuture(channelFuture);
        Assert.assertFalse(future.isDone());

        channelFuture.setSuccess();
        Assert.assertEquals(future.get(1,  TimeUnit.SECONDS), channel);
    }

    @Test
    public void toCompletableFuture_shouldCompleteExceptionally_channelFutureCompletedBefore() throws Exception {
        Exception failure = new Exception();
        channelFuture.setFailure(failure);
        try {
            ChannelFutures.toCompletableFuture(channelFuture).get(1, TimeUnit.SECONDS);
            Assert.fail("Should complete exceptionally");
        } catch (ExecutionException e) {
            Assert.assertSame(e.getCause(), failure);
        }
    }

    @Test
    public void toCompletableFuture_shouldCompleteExceptionally_channelFutureCompletedAfter() throws Exception {
        CompletableFuture<Channel> future = ChannelFutures.toCompletableFuture(channelFuture);
        Assert.assertFalse(future.isDone());

        Exception failure = new Exception();
        channelFuture.setFailure(failure);
        try {
            future.get(1, TimeUnit.SECONDS);
            Assert.fail("Should complete exceptionally");
        } catch (ExecutionException e) {
            Assert.assertSame(e.getCause(), failure);
        }
    }

}
