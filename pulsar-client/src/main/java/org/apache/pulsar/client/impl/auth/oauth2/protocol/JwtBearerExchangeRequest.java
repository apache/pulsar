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
package org.apache.pulsar.client.impl.auth.oauth2.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.net.URI;
import java.security.PrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import lombok.Builder;
import lombok.Data;

/**
 * A token request based on the exchange of JWT Bearer Token.
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7523#section-2.1">OAuth 2.0 RFC 7523, section 2.2</a>
 */
@Data
@Builder
public class JwtBearerExchangeRequest {

    @JsonProperty("assertion")
    private String assertion;

    public static String generateJWT(String clientId,
                                      String audience,
                                      String privateKeyStringUrl,
                                      String signatureAlgorithm,
                                      long ttlMillis)
            throws Exception {
        URI privateKeyStringUri = URI.create(privateKeyStringUrl);
        PrivateKey privateKey = PrivateKeyReader.getPrivateKey(privateKeyStringUri);
        Instant now = Instant.now();

        // per https://developer.okta.com/docs/guides/build-self-signed-jwt/java/main/#gather-claims-information
        // issuer and subject must be the same as client_id
        JwtBuilder builder = Jwts.builder()
                .setAudience(audience)
                .setIssuedAt(Date.from(now))
                .setExpiration(Date.from(now.plus(ttlMillis, ChronoUnit.MILLIS)))
                .setIssuer(clientId)
                .setSubject(clientId)
                .signWith(privateKey, SignatureAlgorithm.forName(signatureAlgorithm));

        return builder.compact();
    }
}
