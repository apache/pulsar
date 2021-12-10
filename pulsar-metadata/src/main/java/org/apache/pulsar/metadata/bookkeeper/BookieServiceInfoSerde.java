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
package org.apache.pulsar.metadata.bookkeeper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.proto.DataFormats.BookieServiceInfoFormat;
import org.apache.bookkeeper.server.service.BookieService;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.Stat;

@Slf4j
public class BookieServiceInfoSerde implements MetadataSerde<BookieServiceInfo> {

    private BookieServiceInfoSerde() {
    }

    static final BookieServiceInfoSerde INSTANCE = new BookieServiceInfoSerde();

    @Override
    public byte[] serialize(String path, BookieServiceInfo bookieServiceInfo) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("serialize BookieServiceInfo {}", bookieServiceInfo);
        }
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            BookieServiceInfoFormat.Builder builder = BookieServiceInfoFormat.newBuilder();
            List<BookieServiceInfoFormat.Endpoint> bsiEndpoints = bookieServiceInfo.getEndpoints().stream()
                    .map(e -> BookieServiceInfoFormat.Endpoint.newBuilder()
                                .setId(e.getId())
                                .setPort(e.getPort())
                                .setHost(e.getHost())
                                .setProtocol(e.getProtocol())
                                .addAllAuth(e.getAuth())
                                .addAllExtensions(e.getExtensions())
                                .build())
                    .collect(Collectors.toList());

            builder.addAllEndpoints(bsiEndpoints);
            builder.putAllProperties(bookieServiceInfo.getProperties());

            builder.build().writeTo(os);
            return os.toByteArray();
        }
    }

    @Override
    public BookieServiceInfo deserialize(String path, byte[] content, Stat stat) throws IOException {
        return null;
    }
}
