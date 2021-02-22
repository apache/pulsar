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
package org.apache.pulsar.broker.service;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.Data;

@Data
public class SendMessageInfo {
    private int totalMessages;
    private long totalBytes;
    private int totalChunkedMessages;

    private SendMessageInfo() {
        // Private constructor so that all usages are through the thread-local instance
    }

    public static SendMessageInfo getThreadLocal() {
        SendMessageInfo smi = THREAD_LOCAL.get();
        smi.totalMessages = 0;
        smi.totalBytes = 0;
        smi.totalChunkedMessages = 0;
        return smi;
    }

    private static final FastThreadLocal<SendMessageInfo> THREAD_LOCAL = new FastThreadLocal<SendMessageInfo>() {
        protected SendMessageInfo initialValue() throws Exception {
            return new SendMessageInfo();
        };
    };

}
