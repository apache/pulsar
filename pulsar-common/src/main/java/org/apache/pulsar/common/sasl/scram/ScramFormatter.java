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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.naming.directory.AttributeModificationException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Locale;
import java.util.Properties;

/**
 * ScramFormatter
 */
public class ScramFormatter {

    private static final String SALT = "salt";

    private static final String STORED_KEY = "stored_key";

    private static final String SERVER_KEY = "server_key";

    private static final String ITERATIONS = "iterations";


    private final MessageDigest messageDigest;

    private final Mac mac;

    private final SecureRandom random;


    public ScramFormatter() {
        try {
            this.random = new SecureRandom();
            this.messageDigest = MessageDigest.getInstance("SHA-256");
            this.mac = Mac.getInstance("HmacSHA256");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] hmac(byte[] key, byte[] bytes) throws InvalidKeyException {
        mac.init(new SecretKeySpec(key, mac.getAlgorithm()));
        return mac.doFinal(bytes);
    }

    public byte[] hash(byte[] str) {
        return messageDigest.digest(str);
    }

    public byte[] xor(byte[] first, byte[] second) {
        if (first.length != second.length)
            throw new IllegalArgumentException("Argument arrays must be of the same length");
        byte[] result = new byte[first.length];
        for (int i = 0; i < result.length; i++)
            result[i] = (byte) (first[i] ^ second[i]);
        return result;
    }

    public byte[] hi(byte[] str, byte[] salt, int iterations) throws InvalidKeyException {
        mac.init(new SecretKeySpec(str, mac.getAlgorithm()));
        mac.update(salt);
        byte[] u1 = mac.doFinal(new byte[]{0, 0, 0, 1});
        byte[] prev = u1;
        byte[] result = u1;
        for (int i = 2; i <= iterations; i++) {
            byte[] ui = hmac(str, prev);
            result = xor(result, ui);
            prev = ui;
        }
        return result;
    }

    public byte[] clientProof(byte[] saltedPassword, String authMessage) throws InvalidKeyException {
        byte[] clientKey = clientKey(saltedPassword);
        byte[] storedKey = hash(clientKey);
        byte[] clientSignature = hmac(storedKey, authMessage.getBytes(StandardCharsets.UTF_8));
        return xor(clientKey, clientSignature);
    }

    public byte[] normalize(String str) {
        return toBytes(str);
    }

    public byte[] saltedPassword(String password, byte[] salt, int iterations) throws InvalidKeyException {
        return hi(normalize(password), salt, iterations);
    }

    public byte[] clientKey(byte[] saltedPassword) throws InvalidKeyException {
        return hmac(saltedPassword, toBytes("Client Key"));
    }

    public byte[] storedKey(byte[] clientKey) {
        return hash(clientKey);
    }

    public byte[] storedKey(byte[] clientSignature, byte[] clientProof) {
        return hash(xor(clientSignature, clientProof));
    }

    public byte[] serverKey(byte[] saltedPassword) throws InvalidKeyException {
        return hmac(saltedPassword, toBytes("Server Key"));
    }

    public byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }


    public static String credentialToString(ScramCredential credential) {
        return String.format(Locale.ROOT, "%s=%s,%s=%s,%s=%s,%s=%d", SALT,
                Base64.getEncoder().encodeToString(credential.salt()), STORED_KEY,
                Base64.getEncoder().encodeToString(credential.storedKey()), SERVER_KEY,
                Base64.getEncoder().encodeToString(credential.serverKey()), ITERATIONS, credential.iterations());
    }

    public static ScramCredential credentialFromString(String str) {
        Properties props = toProps(str);
        if (props.size() != 4 || !props.containsKey(SALT) || !props.containsKey(STORED_KEY)
                || !props.containsKey(SERVER_KEY) || !props.containsKey(ITERATIONS)) {
            throw new IllegalArgumentException("Credentials not valid: " + str);
        }
        byte[] salt = Base64.getDecoder().decode(props.getProperty(SALT));
        byte[] storedKey = Base64.getDecoder().decode(props.getProperty(STORED_KEY));
        byte[] serverKey = Base64.getDecoder().decode(props.getProperty(SERVER_KEY));
        int iterations = Integer.parseInt(props.getProperty(ITERATIONS));
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }

    private static Properties toProps(String str) {
        Properties props = new Properties();
        String[] tokens = str.split(",");
        for (String token : tokens) {
            int index = token.indexOf('=');
            if (index <= 0)
                throw new IllegalArgumentException("Credentials not valid: " + str);
            props.put(token.substring(0, index), token.substring(index + 1));
        }
        return props;
    }


    //Tool
    public ScramCredential generateCredential(String password, int iterations)
            throws AttributeModificationException, InvalidKeyException {
        byte[] salt = secureRandomBytes();
        byte[] saltedPassword = saltedPassword(password, salt, iterations);
        byte[] clientKey = clientKey(saltedPassword);
        byte[] storedKey = storedKey(clientKey);
        byte[] serverKey = serverKey(saltedPassword);
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }

    public String secureRandomString() {
        return new BigInteger(130, random).toString(Character.MAX_RADIX);
    }

    public byte[] secureRandomBytes() {
        return toBytes(secureRandomString());
    }
}
