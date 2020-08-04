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
package org.apache.pulsar.client.impl.auth.oauth2;

import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.Metadata;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.MetadataResolver;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * An abstract OAuth 2.0 authorization flow.
 */
@Slf4j
abstract class FlowBase implements Flow {

    private static final long serialVersionUID = 1L;

    protected final URL issuerUrl;

    protected transient Metadata metadata;

    protected FlowBase(URL issuerUrl) {
        this.issuerUrl = issuerUrl;
    }

    public void initialize() throws PulsarClientException {
        try {
            this.metadata = createMetadataResolver().resolve();
        } catch (IOException e) {
            log.error("Unable to retrieve OAuth 2.0 server metadata", e);
            throw new PulsarClientException.AuthenticationException("Unable to retrieve OAuth 2.0 server metadata");
        }
    }

    protected MetadataResolver createMetadataResolver() {
        return DefaultMetadataResolver.fromIssuerUrl(issuerUrl);
    }

    static String parseParameterString(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        return s;
    }

    static URL parseParameterUrl(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed configuration parameter: " + name);
        }
    }
}
