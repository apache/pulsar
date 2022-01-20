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

import static com.google.common.base.Joiner.on;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.websocket.WebSocketError;

/**
 * Represent result of publishing a single message.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
public class ProducerAck {

    // Message publishing result
    public String result;

    // Error message if fail to publish a message.
    public String errorMsg;

    public String messageId;

    public String context;

    // Indicating if error is retriable error.
    public int errorCode;

    // Version of schema used to encode the message.
    public long schemaVersion;

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
