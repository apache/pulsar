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
package org.apache.pulsar.client.api;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * HTTP response returned to authentication providers.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public final class AuthenticationHttpResponse {
    private final int statusCode;
    private final String statusText;
    private final byte[] body;

    public AuthenticationHttpResponse(int statusCode, String statusText, byte[] body) {
        this.statusCode = statusCode;
        this.statusText = statusText;
        this.body = body == null ? new byte[0] : body.clone();
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusText() {
        return statusText;
    }

    public byte[] getBody() {
        return body.clone();
    }

    public String getBodyAsString(Charset charset) {
        return new String(body, charset);
    }

    public InputStream getBodyAsStream() {
        return new ByteArrayInputStream(body);
    }
}
