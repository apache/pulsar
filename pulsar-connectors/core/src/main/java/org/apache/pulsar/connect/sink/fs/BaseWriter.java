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

import org.apache.pulsar.common.io.FileSystems;
import org.apache.pulsar.common.io.fs.ResourceId;
import org.apache.pulsar.common.io.util.IoUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

public abstract class BaseWriter implements Writer {

    private static final Logger LOG = LoggerFactory.getLogger(BaseWriter.class);

    private OutputStream stream;

    @Override
    public void open(String path) throws IOException {
        if (isOpen()) {
            throw new IOException("writer is already open");
        }
        final ResourceId resourceId = FileSystems.matchNewResource(path, false);
        stream = Channels.newOutputStream(FileSystems.create(resourceId));
    }

    @Override
    public void close() throws IOException {
        IoUtils.close(stream);
        stream = null;
    }

    @Override
    public void flush() throws IOException {
        if (isOpen()) {
            getStream().flush();
        }
    }

    @Override
    public boolean isOpen() {
        return stream != null;
    }

    protected OutputStream getStream() {
        // TODO check for null stream?
        return stream;
    }

    protected void throwIfNotOpen() throws IOException {
        if (!isOpen()) {
            throw new IOException("writer has not been opened.");
        }
    }
}
