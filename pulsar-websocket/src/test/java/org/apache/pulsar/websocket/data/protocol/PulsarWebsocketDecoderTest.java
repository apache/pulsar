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
package org.apache.pulsar.websocket.data.protocol;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.Getter;
import org.apache.pulsar.websocket.data.CommandAuthResponse;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PulsarWebsocketDecoderTest {

    static class MockedPulsarWebsocketDecoder extends PulsarWebsocketDecoder {

        @Getter
        private CommandAuthResponse commandAuthResponse;

        @Override
        public void handleAuthResponse(CommandAuthResponse commandAuthResponse) {
            this.commandAuthResponse = commandAuthResponse;
        }
    }

    @Test
    public void testHandleAuthResponseWithoutClientVersion() throws Exception {
        Path messagePath = Paths.get(this.getClass().getClassLoader().getResource("AUTH_RESPONSE.json").toURI());
        String message = Files.readString(messagePath, StandardCharsets.UTF_8);
        MockedPulsarWebsocketDecoder websocketDecoder = new MockedPulsarWebsocketDecoder();

        websocketDecoder.onWebSocketText(message);
        Assert.assertEquals(
                websocketDecoder.getCommandAuthResponse().getAuthResponse().getResponse().getAuthMethodName(), "token");
        Assert.assertEquals(websocketDecoder.getCommandAuthResponse().getAuthResponse().getResponse().getAuthData(),
                "NewAccessToken");
    }
}
