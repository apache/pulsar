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
import io.jsonwebtoken.security.Keys;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import lombok.Cleanup;

import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;

public class TokensCliUtils {

    public static class Arguments {
        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
    }

    @Parameters(commandDescription = "Create a new secret key or pair of keys")
    public static class CommandCreateSecretKey {
        @Parameter(names = { "-a",
                "--signature-algorithm" }, description = "The signature algorithm for the new secret key. "
                        + "Default is 'HS256' for secret key and 'RS256' for key pairs")
        SignatureAlgorithm algorithm;

        @Parameter(names = { "-p",
                "--key-pair" }, description = "Generate a pair of keys (public and private) instead of a single secret key")
        private boolean keyPair = false;

        public void run() {
            if (algorithm == null) {
                if (keyPair) {
                    algorithm = SignatureAlgorithm.RS256;
                } else {
                    algorithm = SignatureAlgorithm.HS256;
                }
            }

            if (keyPair == false) {
                System.out.println("secret-key " + AuthTokenUtils.createSecretKey(algorithm));
            } else {
                KeyPair pair = Keys.keyPairFor(algorithm);
                System.out.println("public-key " + AuthTokenUtils.encodeKey(pair.getPublic()));
                System.out.println("private-key " + AuthTokenUtils.encodeKey(pair.getPrivate()));
            }
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

        @Parameter(names = { "-i",
                "--stdin" }, description = "Read secret key from standard input")
        private Boolean stdin = false;

        @Parameter(names = { "-f",
                "--secret-key-file" }, description = "Read secret key from a file")
        private String secretKeyFile;

        public void run() throws Exception {
            String secretKey;
            if (stdin) {
                @Cleanup
                BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
                secretKey = r.readLine();
            } else if (secretKeyFile != null) {
                secretKey = new String(Files.readAllBytes(Paths.get(secretKeyFile)), Charsets.UTF_8);
            } else if (System.getenv("SECRET_KEY") != null) {
                secretKey = System.getenv("SECRET_KEY");
            } else {
                System.err.println(
                        "Secret key needs to be either passed through `--stdin`, `--secret-key-file` or by `SECRET_KEY` environment variable");
                System.exit(1);
                return;
            }

            SecretKey key = AuthTokenUtils.deserializeSecretKey(secretKey);

            Optional<Date> optExpiryTime = Optional.empty();
            if (expiryTime != null) {
                long relativeTimeMillis = parseRelativeTimeInMillis(expiryTime);
                optExpiryTime = Optional.of(new Date(System.currentTimeMillis() + relativeTimeMillis));
            }

            String token = AuthTokenUtils.createToken(key, subject, optExpiryTime);
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

    public static long parseRelativeTimeInMillis(String relativeTime) {
        if (relativeTime.isEmpty()) {
            throw new IllegalArgumentException("exipiry time cannot be empty");
        }

        char lastChar = relativeTime.charAt(relativeTime.length() - 1);

        if (!Character.isAlphabetic(lastChar)) {
            throw new IllegalArgumentException("Relative time should contain time unit. eg: 3h or 5d");
        }

        long duration = Long.parseLong(relativeTime.substring(0, relativeTime.length() - 1));

        switch (lastChar) {
        case 's':
            return TimeUnit.SECONDS.toMillis(duration);
        case 'm':
            return TimeUnit.MINUTES.toMillis(duration);
        case 'h':
            return TimeUnit.HOURS.toMillis(duration);
        case 'd':
            return TimeUnit.DAYS.toMillis(duration);
        // No unit for months
        case 'y':
            return 365 * TimeUnit.DAYS.toMillis(duration);
        default:
            throw new IllegalArgumentException("Invalid time unit '" + lastChar + "'");
        }
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander(arguments);

        CommandCreateSecretKey commandCreateSecretKey = new CommandCreateSecretKey();
        jcommander.addCommand("create-secret-key", commandCreateSecretKey);

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
