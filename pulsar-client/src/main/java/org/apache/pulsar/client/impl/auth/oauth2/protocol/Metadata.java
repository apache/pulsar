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
package org.apache.pulsar.client.impl.auth.oauth2.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents OAuth 2.0 Server Metadata.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Metadata {
    @JsonProperty("issuer")
    private URL issuer;

    @JsonProperty("authorization_endpoint")
    private URL authorizationEndpoint;

    @JsonProperty("token_endpoint")
    private URL tokenEndpoint;

    @JsonProperty("userinfo_endpoint")
    private URL userInfoEndpoint;

    @JsonProperty("revocation_endpoint")
    private URL revocationEndpoint;

    @JsonProperty("jwks_uri")
    private URL jwksUri;

    @JsonProperty("device_authorization_endpoint")
    private URL deviceAuthorizationEndpoint;
}
