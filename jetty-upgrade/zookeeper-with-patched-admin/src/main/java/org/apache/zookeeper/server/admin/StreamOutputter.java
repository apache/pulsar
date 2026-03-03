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
package org.apache.zookeeper.server.admin;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.zookeeper.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for streaming data out.
 */
public class StreamOutputter implements CommandOutputter {
    private static final Logger LOG = LoggerFactory.getLogger(StreamOutputter.class);
    private final String clientIP;

    public StreamOutputter(final String clientIP) {
        this.clientIP = clientIP;
    }

    @Override
    public String getContentType() {
        return "application/octet-stream";
    }

    @Override
    public void output(final CommandResponse response, final OutputStream os) {
        try (final InputStream is = response.getInputStream()) {
            IOUtils.copyBytes(is, os, 1024, true);
        } catch (final IOException e) {
            LOG.warn("Exception streaming out data to {}", clientIP, e);
        }
    }
}
