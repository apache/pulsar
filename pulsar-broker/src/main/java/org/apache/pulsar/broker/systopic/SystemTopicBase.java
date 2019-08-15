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
package org.apache.pulsar.broker.systopic;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class SystemTopicBase implements SystemTopic {

    protected final TopicName topicName;
    protected final PulsarClient client;

    protected Writer writer;
    protected final List<Reader> readers;

    public SystemTopicBase(PulsarClient client, TopicName topicName) {
        this.client = client;
        this.topicName = topicName;
        this.readers = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public TopicName getTopicName() {
        return topicName;
    }

    protected abstract Writer createWriter() throws PulsarClientException;

    protected abstract Reader createReaderInternal() throws PulsarClientException;

    @Override
    public Writer getWriter() throws PulsarClientException {
        synchronized (this) {
            if (writer == null) {
                writer = createWriter();
            }
        }
        return writer;
    }

    @Override
    public Reader createReader() throws PulsarClientException {
        Reader reader = createReaderInternal();
        readers.add(reader);
        return reader;
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
            for (Reader reader : readers) {
                reader.close();
            }
        } catch (IOException e) {
            log.error("Close system topic [{}] error.", topicName.toString(), e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SystemTopicBase.class);
}
