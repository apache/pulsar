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
package org.apache.pulsar.common.sasl.scram;

import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.naming.AuthenticationException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

@Slf4j
public class HmacRoleTokenSigner {
    public static final String SIGNATURE = "&s=";

    private byte[] key;

    private static final int HMAC_KEY_LENGTH = 32;

    /**
     * Creates a SaslRoleTokenSigner instance using the specified secret.
     *
     * @param secret secret to use for creating the digest.
     */
    public HmacRoleTokenSigner(String secret) {
        //a valid HmacKey Length should be more than 64
        if (secret == null || secret.length() < HMAC_KEY_LENGTH) {
            throw new IllegalArgumentException("secret cannot be NULL or less than " + HMAC_KEY_LENGTH);
        }
        this.key = StringUtil.decodeHexDump(secret);
    }

    /**
     * Returns a signed string.
     * <p/>
     * The signature '&s=SIGNATURE' is appended at the end of the string.
     *
     * @param str string to sign.
     * @return the signed string.
     */
    public String sign(String str) {
        if (str == null || str.length() == 0) {
            throw new IllegalArgumentException("NULL or empty string to sign");
        }
        String signature = computeSignature(str);
        return str + SIGNATURE + signature;
    }

    /**
     * Returns the signature of a string.
     *
     * @param str string to sign.
     * @return the signature(HMAC-SHA256) for the string.
     */
    public String computeSignature(String str) {
        try {

            SecretKeySpec secretKey = new SecretKeySpec(key, "HmacSHA256");
            Mac mac = Mac.getInstance(secretKey.getAlgorithm());
            mac.init(secretKey);
            byte[] digest = mac.doFinal(str.getBytes(StandardCharsets.UTF_8));

            return StringUtil.toHexString(digest).toUpperCase(Locale.ROOT);

        } catch (NoSuchAlgorithmException | InvalidKeyException ex) {
            throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
        }
    }
}
