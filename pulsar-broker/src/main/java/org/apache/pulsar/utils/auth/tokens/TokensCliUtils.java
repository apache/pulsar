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
package org.apache.pulsar.utils.auth.tokens;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Charsets;

import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.io.Encoders;
import io.jsonwebtoken.security.Keys;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyPair;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import lombok.Cleanup;

import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.util.RelativeTimeUtil;

public class TokensCliUtils {

    public static class Arguments {
        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
    }

    @Parameters(commandDescription = "Create a new secret key")
    public static class CommandCreateSecretKey {
        @Parameter(names = { "-a",
                "--signature-algorithm" }, description = "The signature algorithm for the new secret key.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.HS256;

        @Parameter(names = { "-o",
                "--output" }, description = "Write the secret key to a file instead of stdout")
        String outputFile;

        @Parameter(names = {
                "-b", "--base-64" }, description = "Encode the key in base64")
        boolean base64 = false;

        public void run() throws IOException {
            SecretKey secretKey = AuthTokenUtils.createSecretKey(algorithm);
            byte[] encoded = secretKey.getEncoded();

            if (base64) {
                encoded = Encoders.BASE64.encode(encoded).getBytes();
            }

            if (outputFile != null) {
                Files.write(Paths.get(outputFile), encoded);
            } else {
                System.out.write(encoded);
            }
        }
    }

    @Parameters(commandDescription = "Create a new or pair of keys public/private")
    public static class CommandCreateKeyPair {
        @Parameter(names = { "-a",
                "--signature-algorithm" }, description = "The signature algorithm for the new key pair.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

        @Parameter(names = {
                "--output-private-key" }, description = "File where to write the private key", required = true)
        String privateKeyFile;
        @Parameter(names = {
                "--output-public-key" }, description = "File where to write the public key", required = true)
        String publicKeyFile;

        public void run() throws IOException {
            KeyPair pair = Keys.keyPairFor(algorithm);

            Files.write(Paths.get(publicKeyFile), pair.getPublic().getEncoded());
            Files.write(Paths.get(privateKeyFile), pair.getPrivate().getEncoded());
        }
    }

    @Parameters(commandDescription = "Create a new token")
    public static class CommandCreateToken {

        @Parameter(names = { "-s",
                "--subject" }, description = "Specify the 'subject' or 'principal' associate with this token", required = true)
        private String subject;

        @Parameter(names = { "-e",
                "--expiry-time" }, description = "Relative expiry time for the token (eg: 1h, 3d, 10y). (m=minutes) Default: no expiration")
        private String expiryTime;

        @Parameter(names = { "-pk",
                "--is-private-key" }, description = "Indicate the signing key is a private key (rather than a symmetric secret key)")
        private Boolean isPrivateKey = false;

        @Parameter(names = { "-k",
                "--signing-key" }, description = "Pass the signing key. This can either be: data:, file:, etc..", required = true)
        private String key;

        public void run() throws Exception {
            byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(key);

            Key signingKey;

            if (isPrivateKey) {
                signingKey = AuthTokenUtils.decodePrivateKey(encodedKey);
            } else {
                signingKey = AuthTokenUtils.decodeSecretKey(encodedKey);
            }

            Optional<Date> optExpiryTime = Optional.empty();
            if (expiryTime != null) {
                long relativeTimeMillis = TimeUnit.SECONDS
                        .toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(expiryTime));
                optExpiryTime = Optional.of(new Date(System.currentTimeMillis() + relativeTimeMillis));
            }

            String token = AuthTokenUtils.createToken(signingKey, subject, optExpiryTime);
            System.out.println(token);
        }
    }

    @Parameters(commandDescription = "Show the content of token")
    public static class CommandShowToken {

        @Parameter(description = "The token string", arity = 1)
        private java.util.List<String> args;

        @Parameter(names = { "-i",
                "--stdin" }, description = "Read token from standard input")
        private Boolean stdin = false;

        @Parameter(names = { "-f",
                "--token-file" }, description = "Read tokn from a file")
        private String tokenFile;

        public void run() throws Exception {
            String token;
            if (args != null) {
                token = args.get(0);
            } else if (stdin) {
                @Cleanup
                BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
                token = r.readLine();
            } else if (tokenFile != null) {
                token = new String(Files.readAllBytes(Paths.get(tokenFile)), Charsets.UTF_8);
            } else if (System.getenv("TOKEN") != null) {
                token = System.getenv("TOKEN");
            } else {
                System.err.println(
                        "Token needs to be either passed through `--stdin`, `--token-file` or by `TOKEN` environment variable");
                System.exit(1);
                return;
            }

            String[] parts = token.split("\\.");
            System.out.println(new String(Decoders.BASE64URL.decode(parts[0])));
            System.out.println("---");
            System.out.println(new String(Decoders.BASE64URL.decode(parts[1])));
        }
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander(arguments);

        CommandCreateSecretKey commandCreateSecretKey = new CommandCreateSecretKey();
        jcommander.addCommand("create-secret-key", commandCreateSecretKey);

        CommandCreateKeyPair commandCreateKeyPair = new CommandCreateKeyPair();
        jcommander.addCommand("create-key-pair", commandCreateKeyPair);

        CommandCreateToken commandCreateToken = new CommandCreateToken();
        jcommander.addCommand("create", commandCreateToken);

        CommandShowToken commandShowToken = new CommandShowToken();
        jcommander.addCommand("show", commandShowToken);

        try {
            jcommander.parse(args);

            if (arguments.help || jcommander.getParsedCommand() == null) {
                jcommander.usage();
                System.exit(1);
            }
        } catch (Exception e) {
            jcommander.usage();
            System.err.println(e);
            System.exit(1);
        }

        String cmd = jcommander.getParsedCommand();

        if (cmd.equals("create-secret-key")) {
            commandCreateSecretKey.run();
        } else if (cmd.equals("create")) {
            commandCreateToken.run();
        } else if (cmd.equals("show")) {
            commandShowToken.run();
        } else {
            System.err.println("Invalid command: " + cmd);
            System.exit(1);
        }
    }
}
