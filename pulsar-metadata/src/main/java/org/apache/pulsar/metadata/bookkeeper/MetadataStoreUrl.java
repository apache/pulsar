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
package org.apache.pulsar.metadata.bookkeeper;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

record MetadataStoreUrl(String url, Map<String, String> configParams) {
    static MetadataStoreUrl parse(String metadataServiceUri) {
        String metadataStoreUrl = removeMetadataStoreScheme(metadataServiceUri);
        int queryStart = metadataStoreUrl.indexOf('?');
        if (queryStart < 0) {
            return new MetadataStoreUrl(metadataStoreUrl.replace(";", ","), Map.of());
        }

        String url = metadataStoreUrl.substring(0, queryStart).replace(";", ",");
        String query = metadataStoreUrl.substring(queryStart + 1);
        if (query.isEmpty()) {
            return new MetadataStoreUrl(url, Map.of());
        }

        Map<String, String> configParams = new LinkedHashMap<>();
        List<String> providerParams = new ArrayList<>();
        for (String param : query.split("&", -1)) {
            if (param.isEmpty()) {
                continue;
            }
            int separator = param.indexOf('=');
            String key = decodeQueryParam(separator > 0 ? param.substring(0, separator) : param);
            if (!MetadataStoreConfigQueryParams.contains(key)) {
                providerParams.add(param);
                continue;
            }
            if (separator <= 0) {
                throw new IllegalArgumentException("Invalid MetadataStoreConfig query parameter '" + param
                        + "'. Expected key=value");
            }

            String value = decodeQueryParam(param.substring(separator + 1));
            configParams.put(key, value);
        }

        if (!providerParams.isEmpty()) {
            url += "?" + String.join("&", providerParams);
        }
        return new MetadataStoreUrl(url, Map.copyOf(configParams));
    }

    private static String removeMetadataStoreScheme(String metadataServiceUri) {
        String prefix = AbstractMetadataDriver.METADATA_STORE_SCHEME + ":";
        if (metadataServiceUri.startsWith(prefix)) {
            return metadataServiceUri.substring(prefix.length());
        }
        return metadataServiceUri;
    }

    private static String decodeQueryParam(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
