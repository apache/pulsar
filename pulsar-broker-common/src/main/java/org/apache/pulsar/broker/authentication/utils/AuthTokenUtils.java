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
package org.apache.pulsar.broker.authentication.utils;

import com.google.common.io.ByteStreams;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.io.Encoders;
import io.jsonwebtoken.security.Keys;

import java.io.IOException;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Date;
import java.util.Optional;

import javax.crypto.SecretKey;

import lombok.experimental.UtilityClass;

import org.apache.pulsar.client.api.url.URL;

@UtilityClass
public class AuthTokenUtils {

    public static SecretKey createSecretKey(SignatureAlgorithm signatureAlgorithm) {
        return Keys.secretKeyFor(signatureAlgorithm);
    }

    public static SecretKey decodeSecretKey(byte[] secretKey) {
        return Keys.hmacShaKeyFor(secretKey);
    }

    public static PrivateKey decodePrivateKey(byte[] key, SignatureAlgorithm algType) throws IOException {
        try {
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(key);
            KeyFactory kf = KeyFactory.getInstance(keyTypeForSignatureAlgorithm(algType));
            return kf.generatePrivate(spec);
        } catch (Exception e) {
            throw new IOException("Failed to decode private key", e);
        }
    }


    public static PublicKey decodePublicKey(byte[] key, SignatureAlgorithm algType) throws IOException {
        try {
            X509EncodedKeySpec spec = new X509EncodedKeySpec(key);
            KeyFactory kf = KeyFactory.getInstance(keyTypeForSignatureAlgorithm(algType));
            return kf.generatePublic(spec);
        } catch (Exception e) {
            throw new IOException("Failed to decode public key", e);
        }
    }

    private static String keyTypeForSignatureAlgorithm(SignatureAlgorithm alg) {
        if (alg.getFamilyName().equals("RSA")) {
            return "RSA";
        } else if (alg.getFamilyName().equals("ECDSA")) {
            return "EC";
        } else {
            String msg = "The " + alg.name() + " algorithm does not support Key Pairs.";
            throw new IllegalArgumentException(msg);
        }
    }

    public static String encodeKeyBase64(Key key) {
        return Encoders.BASE64.encode(key.getEncoded());
    }

    public static String createToken(Key signingKey, String subject, Optional<Date> expiryTime) {
        JwtBuilder builder = Jwts.builder()
                .setSubject(subject)
                .signWith(signingKey);

        if (expiryTime.isPresent()) {
            builder.setExpiration(expiryTime.get());
        }

        return builder.compact();
    }

    public static byte[] readKeyFromUrl(String keyConfUrl) throws IOException {
        if (keyConfUrl.startsWith("data:") || keyConfUrl.startsWith("file:")) {
            try {
                return ByteStreams.toByteArray((InputStream) new URL(keyConfUrl).getContent());
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            // Assume the key content was passed in base64
            return Decoders.BASE64.decode(keyConfUrl);
        }
    }
}
