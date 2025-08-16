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
package org.apache.pulsar.client.impl.auth.oauth2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;


/**
 * A JSON object representing a credentials file.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyFile {

    @JsonProperty("type")
    private String type;

    @JsonProperty("client_id")
    private String clientId;

    @JsonProperty("client_secret")
    private String clientSecret;

    @JsonProperty("client_email")
    private String clientEmail;

    @JsonProperty("issuer_url")
    private String issuerUrl;

    public String toJson() throws IOException {
        return ObjectMapperFactory.getMapperWithIncludeAlways().writer().writeValueAsString(this);
    }

    public static KeyFile fromJson(String value) throws IOException {
        return ObjectMapperFactory.getMapper().reader().readValue(value, KeyFile.class);
    }

    public static KeyFile fromJson(Reader value) throws IOException {
        return ObjectMapperFactory.getMapper().reader().readValue(value, KeyFile.class);
    }

    /**
     * Loads the private key from the given URL.
     * @param privateKeyURL
     * @return
     * @throws IOException
     */
    public static KeyFile loadPrivateKey(String privateKeyURL) throws IOException {
        try {
            URLConnection urlConnection = new org.apache.pulsar.client.api.url.URL(privateKeyURL).openConnection();
            try {
                String protocol = urlConnection.getURL().getProtocol();
                String contentType = urlConnection.getContentType();
                if ("data".equals(protocol) && !"application/json".equals(contentType)) {
                    throw new IllegalArgumentException(
                            "Unsupported media type or encoding format: " + urlConnection.getContentType());
                }
                KeyFile privateKey;
                try (Reader r = new InputStreamReader((InputStream) urlConnection.getContent(),
                        StandardCharsets.UTF_8)) {
                    privateKey = KeyFile.fromJson(r);
                }
                return privateKey;
            } finally {
                IOUtils.close(urlConnection);
            }
        } catch (URISyntaxException | InstantiationException | IllegalAccessException e) {
            throw new IOException("Invalid privateKey format", e);
        }
    }
}
