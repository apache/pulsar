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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.testng.annotations.Test;

public class OptionalProxyProtocolDecoderTest {

    @Test
    public void incompleteProxyProtocolBufferIsReleasedWhenChannelCloses() {
        EmbeddedChannel channel = new EmbeddedChannel(new OptionalProxyProtocolDecoder());
        ByteBuf incompleteHeader = Unpooled.directBuffer(
                OptionalProxyProtocolDecoder.MIN_BYTES_SIZE_TO_DETECT_PROTOCOL - 1).writeZero(1);
        try {
            channel.writeInbound(incompleteHeader);
            channel.close();
            assertThat(incompleteHeader.refCnt())
                    .as("the accumulated HAProxy detection buffer must be released when the channel closes")
                    .isZero();
        } finally {
            ReferenceCountUtil.safeRelease(incompleteHeader);
            channel.finishAndReleaseAll();
        }
    }
}
