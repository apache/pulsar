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
package org.apache.pulsar.connect.sink.fs;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BytesWriter extends BaseWriter {

    private final ByteBuffer header = ByteBuffer.allocate(4);

    @Override
    public void write(Message message) throws IOException {
        final byte[] data = message.getData();
        final int size = data.length;
        header.clear();
        header.putInt(size);

        final OutputStream stream = getStream();
        stream.write(header.array());
        stream.write(message.getData());
    }
}
