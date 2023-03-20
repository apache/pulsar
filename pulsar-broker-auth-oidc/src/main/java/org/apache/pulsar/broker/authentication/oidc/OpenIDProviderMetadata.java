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
package org.apache.pulsar.broker.authentication.oidc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A Simple Class representing the essential fields of the OpenID Provider Metadata.
 * Spec: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata
 * Note that this class is only used for deserializing the JSON metadata response from
 * calling a provider's /.well-known/openid-configuration endpoint.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenIDProviderMetadata {

    private final String issuer;
    private final String jwksUri;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OpenIDProviderMetadata(@JsonProperty("issuer") String issuer, @JsonProperty("jwks_uri") String jwksUri) {
        this.issuer = issuer;
        this.jwksUri = jwksUri;
    }

    @JsonGetter
    public String getIssuer() {
        return issuer;
    }

    @JsonGetter
    public String getJwksUri() {
        return jwksUri;
    }
}
