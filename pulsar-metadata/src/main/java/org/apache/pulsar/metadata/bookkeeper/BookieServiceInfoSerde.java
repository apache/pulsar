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
import org.apache.bookkeeper.discover.BookieServiceInfoUtils;
import org.apache.bookkeeper.proto.DataFormats.BookieServiceInfoFormat;
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
    public BookieServiceInfo deserialize(String path, byte[] bookieServiceInfo, Stat stat) throws IOException {
        // see https://github.com/apache/bookkeeper/blob/
        // 034ef8566ad037937a4d58a28f70631175744f53/bookkeeper-server/
        // src/main/java/org/apache/bookkeeper/discover/ZKRegistrationClient.java#L311
        String bookieId = extractBookiedIdFromPath(path);
        if (bookieServiceInfo == null || bookieServiceInfo.length == 0) {
            return BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookieId);
        }

        BookieServiceInfoFormat builder = BookieServiceInfoFormat.parseFrom(bookieServiceInfo);
        BookieServiceInfo bsi = new BookieServiceInfo();
        List<BookieServiceInfo.Endpoint> endpoints = builder.getEndpointsList().stream()
                .map(e -> {
                    BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
                    endpoint.setId(e.getId());
                    endpoint.setPort(e.getPort());
                    endpoint.setHost(e.getHost());
                    endpoint.setProtocol(e.getProtocol());
                    endpoint.setAuth(e.getAuthList());
                    endpoint.setExtensions(e.getExtensionsList());
                    return endpoint;
                })
                .collect(Collectors.toList());

        bsi.setEndpoints(endpoints);
        bsi.setProperties(builder.getPropertiesMap());

        return bsi;

    }

    /**
     * Extract the BookieId
     * The path should look like /ledgers/available/bookieId
     * or /ledgers/available/readonly/bookieId.
     * But the prefix depends on the configuration.
     * @param path
     * @return the bookieId
     */
    private static String extractBookiedIdFromPath(String path) throws IOException {
        // https://github.com/apache/bookkeeper/blob/
        // 034ef8566ad037937a4d58a28f70631175744f53/bookkeeper-server/
        // src/main/java/org/apache/bookkeeper/discover/ZKRegistrationClient.java#L258
        if (path == null) {
            path = "";
        }
        int last = path.lastIndexOf("/");
        if (last >= 0) {
            return path.substring(last + 1);
        } else {
            throw new IOException("The path " + path + " doesn't look like a valid path for a BookieServiceInfo node");
        }
    }
}
