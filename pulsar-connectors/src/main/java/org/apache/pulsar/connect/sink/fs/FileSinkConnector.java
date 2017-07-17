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
import org.apache.pulsar.common.io.util.IoUtils;
import org.apache.pulsar.connect.api.sink.SinkConnector;
import org.apache.pulsar.connect.config.ConnectorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * /base/path/{date}/output-{time}
 */
public class FileSinkConnector extends SinkConnector {

    private static final Logger LOG = LoggerFactory.getLogger(FileSinkConnector.class);

    private static final String KEY_BASE_PATH = "basepath";
    private static final String KEY_FILE_SIZE_MB = "filesize.mb";

    private static final long MB = 1024L * 1024;
    private static final long DEFAULT_FILE_SIZE_MB = 64;

    private static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";
    private static final String DEFAULT_DATE_FORMAT = "yyyyMMdd";
    private static final String DEFAULT_TIME_FORMAT = "HH-mm-ss";

    private final DateFormat dateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
    private final DateFormat timeFormat = new SimpleDateFormat(DEFAULT_TIME_FORMAT);

    private String basePath;
    private String topic;
    private String subscription;

    private Writer writer;
    private long fileSizeInBytes;
    private long bytesWritten = 0;
    private String fileUri;

    @Override
    public void initialize(Properties properties) {
        topic = properties.getProperty(ConnectorConfiguration.KEY_TOPIC);
        subscription = properties.getProperty(ConnectorConfiguration.KEY_SUBSCRIPTION);
        basePath = properties.getProperty(KEY_BASE_PATH);
        final String fileSize =
                properties.getProperty(KEY_FILE_SIZE_MB,
                        String.valueOf(DEFAULT_FILE_SIZE_MB));

        fileSizeInBytes = Long.parseLong(fileSize) * MB;
    }

    @Override
    public boolean processMessage(Message message) throws IOException {
        Writer writer = getWriterAndOpenIfNecessary();

        writer.write(message);

        bytesWritten += message.getData().length;
        if (bytesWritten >= fileSizeInBytes) {
            commitAndReset();
            return true;
        }

        return false;
    }

    @Override
    public void close() {
        try {
            commitAndReset();
        } catch (IOException e) {
            LOG.warn("failed to commit file when closing", e);
        }
    }

    private String createFileUri() {
        final String base = basePath.endsWith("/") ? basePath : basePath + "/";
        final Date date = new Date();
        return base +
                dateFormat.format(date) + "/" +
                "output-" + timeFormat.format(date);
    }

    private Writer getWriterAndOpenIfNecessary() throws IOException {
        if (writer == null) {
            writer = new BytesWriter();
            fileUri = createFileUri();
            LOG.info("opening file {}", fileUri);
            writer.open(createFileUri());
        }

        return writer;
    }

    private void commitAndReset() throws IOException {
        if (writer != null) {
            writer.flush();
            IoUtils.close(writer);
        }
        if (fileUri != null) {
            LOG.info("file {} committed size {}", fileUri, toMb(bytesWritten));
        }
        writer = null;
        bytesWritten = 0;
        fileUri = null;
    }

    private static double toMb(long size) {
        return (double) size / MB;
    }
}
