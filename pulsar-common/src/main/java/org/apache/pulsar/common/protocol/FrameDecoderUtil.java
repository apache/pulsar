/*
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
package org.apache.pulsar.common.protocol;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.experimental.UtilityClass;

/**
 * Utility class for managing Netty LenghtFieldBasedFrameDecoder instances in a Netty ChannelPipeline
 * for the Pulsar binary protocol.
 */
@UtilityClass
public class FrameDecoderUtil {
    public static final String FRAME_DECODER_HANDLER = "frameDecoder";

    /**
     * Adds a LengthFieldBasedFrameDecoder to the given ChannelPipeline.
     *
     * @param pipeline the ChannelPipeline to which the decoder will be added
     * @param maxMessageSize the maximum size of messages that can be decoded
     */
    public static void addFrameDecoder(ChannelPipeline pipeline, int maxMessageSize) {
        pipeline.addLast(FRAME_DECODER_HANDLER, createFrameDecoder(maxMessageSize));
    }

    /**
     * Replaces the existing LengthFieldBasedFrameDecoder in the given ChannelPipeline with a new one.
     *
     * @param pipeline the ChannelPipeline in which the decoder will be replaced
     * @param maxMessageSize the maximum size of messages that can be decoded
     */
    public static void replaceFrameDecoder(ChannelPipeline pipeline, int maxMessageSize) {
        pipeline.replace(FRAME_DECODER_HANDLER, FRAME_DECODER_HANDLER, createFrameDecoder(maxMessageSize));
    }

    /**
     * Removes the LengthFieldBasedFrameDecoder from the given ChannelPipeline.
     * This is useful in the Pulsar Proxy to remove the decoder before direct proxying of messages without decoding.
     *
     * @param pipeline the ChannelPipeline from which the decoder will be removed
     */
    public static void removeFrameDecoder(ChannelPipeline pipeline) {
        pipeline.remove(FRAME_DECODER_HANDLER);
    }

    private static LengthFieldBasedFrameDecoder createFrameDecoder(int maxMessageSize) {
        return new LengthFieldBasedFrameDecoder(
                maxMessageSize + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4);
    }
}
