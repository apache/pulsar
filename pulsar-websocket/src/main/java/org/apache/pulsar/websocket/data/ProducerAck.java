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
package org.apache.pulsar.websocket.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import static com.google.common.base.Joiner.on;

import org.apache.pulsar.websocket.WebSocketError;

@JsonInclude(Include.NON_NULL)
public class ProducerAck {
    public String result;
    public String errorMsg;
    public String messageId;
    public String context;

    public ProducerAck(String messageId, String context) {
        this.result = "ok";
        this.messageId = messageId;
        this.context = context;
    }

    public ProducerAck(WebSocketError error, String errorMsg, String messageId, String context) {
        this.result = on(':').join("send-error", error.getCode());
        this.errorMsg = errorMsg;
        this.messageId = messageId;
        this.context = context;
    }

}
