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
package org.apache.pulsar.functions.utils;

import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.ServerSocket;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * Utils used for runtime.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

    public static final long getSequenceId(MessageId messageId) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;
        long ledgerId = msgId.getLedgerId();
        long entryId = msgId.getEntryId();

        // Combine ledger id and entry id to form offset
        // Use less than 32 bits to represent entry id since it will get
        // rolled over way before overflowing the max int range
        long offset = (ledgerId << 28) | entryId;
        return offset;
    }

    public static final MessageId getMessageId(long sequenceId) {
        // Demultiplex ledgerId and entryId from offset
        long ledgerId = sequenceId >>> 28;
        long entryId = sequenceId & 0x0F_FF_FF_FFL;

        return new MessageIdImpl(ledgerId, entryId, -1);
    }

    public static String printJson(MessageOrBuilder msg) throws IOException {
        return JsonFormat.printer().print(msg);
    }

    public static void mergeJson(String json, Builder builder) throws IOException {
        JsonFormat.parser().merge(json, builder);
    }

    public static int findAvailablePort() {
        // The logic here is a little flaky. There is no guarantee that this
        // port returned will be available later on when the instance starts
        // TODO(sanjeev):- Fix this
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException ex){
            throw new RuntimeException("No free port found", ex);
        }
    }
}
