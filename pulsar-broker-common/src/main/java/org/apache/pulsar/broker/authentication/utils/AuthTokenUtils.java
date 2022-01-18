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

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.io.DecodingException;
import io.jsonwebtoken.io.Encoders;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
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

        expiryTime.ifPresent(builder::setExpiration);

        return builder.compact();
    }

    public static byte[] readKeyFromUrl(String keyConfUrl) throws IOException {
        if (keyConfUrl.startsWith("data:") || keyConfUrl.startsWith("file:")) {
            try {
                return IOUtils.toByteArray(URL.createURL(keyConfUrl));
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else if (Files.exists(Paths.get(keyConfUrl))) {
            // Assume the key content was passed in a valid file path
            return Files.readAllBytes(Paths.get(keyConfUrl));
        } else if (Base64.isBase64(keyConfUrl.getBytes())) {
            // Assume the key content was passed in base64
            try {
                return Decoders.BASE64.decode(keyConfUrl);
            } catch (DecodingException e) {
                String msg = "Illegal base64 character or Key file " + keyConfUrl + " doesn't exist";
                throw new IOException(msg, e);
            }
        } else {
            String msg = "Secret/Public Key file " + keyConfUrl + " doesn't exist";
            throw new IllegalArgumentException(msg);
        }
    }
}
