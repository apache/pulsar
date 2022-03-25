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

import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.IUsageFormatter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Charsets;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
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
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.common.util.RelativeTimeUtil;

public class TokensCliUtils {

    public static class Arguments {
        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;
    }

    @Parameters(commandDescription = "Create a new secret key")
    public static class CommandCreateSecretKey {
        @Parameter(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the new secret key.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.HS256;

        @Parameter(names = {"-o",
                "--output"}, description = "Write the secret key to a file instead of stdout")
        String outputFile;

        @Parameter(names = {
                "-b", "--base64"}, description = "Encode the key in base64")
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
        @Parameter(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the new key pair.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

        @Parameter(names = {
                "--output-private-key"}, description = "File where to write the private key", required = true)
        String privateKeyFile;
        @Parameter(names = {
                "--output-public-key"}, description = "File where to write the public key", required = true)
        String publicKeyFile;

        public void run() throws IOException {
            KeyPair pair = Keys.keyPairFor(algorithm);

            Files.write(Paths.get(publicKeyFile), pair.getPublic().getEncoded());
            Files.write(Paths.get(privateKeyFile), pair.getPrivate().getEncoded());
        }
    }

    @Parameters(commandDescription = "Create a new token")
    public static class CommandCreateToken {
        @Parameter(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the new key pair.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

        @Parameter(names = {"-s",
                "--subject"},
                description = "Specify the 'subject' or 'principal' associate with this token", required = true)
        private String subject;

        @Parameter(names = {"-e",
                "--expiry-time"},
                description = "Relative expiry time for the token (eg: 1h, 3d, 10y)."
                        + " (m=minutes) Default: no expiration")
        private String expiryTime;

        @Parameter(names = {"-sk",
                "--secret-key"},
                description = "Pass the secret key for signing the token. This can either be: data:, file:, etc..")
        private String secretKey;

        @Parameter(names = {"-pk",
                "--private-key"},
                description = "Pass the private key for signing the token. This can either be: data:, file:, etc..")
        private String privateKey;

        public void run() throws Exception {
            if (secretKey == null && privateKey == null) {
                System.err.println(
                        "Either --secret-key or --private-key needs to be passed for signing a token");
                System.exit(1);
            } else if (secretKey != null && privateKey != null) {
                System.err.println(
                        "Only one of --secret-key and --private-key needs to be passed for signing a token");
                System.exit(1);
            }

            Key signingKey;

            if (privateKey != null) {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(privateKey);
                signingKey = AuthTokenUtils.decodePrivateKey(encodedKey, algorithm);
            } else {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(secretKey);
                signingKey = AuthTokenUtils.decodeSecretKey(encodedKey);
            }

            Optional<Date> optExpiryTime = Optional.empty();
            if (expiryTime != null) {
                long relativeTimeMillis;
                try {
                    relativeTimeMillis = TimeUnit.SECONDS.toMillis(
                            RelativeTimeUtil.parseRelativeTimeInSeconds(expiryTime));
                } catch (IllegalArgumentException exception) {
                    throw new ParameterException(exception.getMessage());
                }
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

        @Parameter(names = {"-i",
                "--stdin"}, description = "Read token from standard input")
        private Boolean stdin = false;

        @Parameter(names = {"-f",
                "--token-file"}, description = "Read token from a file")
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
                        "Token needs to be either passed as an argument or through `--stdin`,"
                                + " `--token-file` or by the `TOKEN` environment variable");
                System.exit(1);
                return;
            }

            String[] parts = token.split("\\.");
            System.out.println(new String(Decoders.BASE64URL.decode(parts[0])));
            System.out.println("---");
            System.out.println(new String(Decoders.BASE64URL.decode(parts[1])));
        }
    }

    @Parameters(commandDescription = "Validate a token against a key")
    public static class CommandValidateToken {

        @Parameter(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the key pair if using public key.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

        @Parameter(description = "The token string", arity = 1)
        private java.util.List<String> args;

        @Parameter(names = {"-i",
                "--stdin"}, description = "Read token from standard input")
        private Boolean stdin = false;

        @Parameter(names = {"-f",
                "--token-file"}, description = "Read token from a file")
        private String tokenFile;

        @Parameter(names = {"-sk",
                "--secret-key"},
                description = "Pass the secret key for validating the token. This can either be: data:, file:, etc..")
        private String secretKey;

        @Parameter(names = {"-pk",
                "--public-key"},
                description = "Pass the public key for validating the token. This can either be: data:, file:, etc..")
        private String publicKey;

        public void run() throws Exception {
            if (secretKey == null && publicKey == null) {
                System.err.println(
                        "Either --secret-key or --public-key needs to be passed for signing a token");
                System.exit(1);
            } else if (secretKey != null && publicKey != null) {
                System.err.println(
                        "Only one of --secret-key and --public-key needs to be passed for signing a token");
                System.exit(1);
            }

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
                        "Token needs to be either passed as an argument or through `--stdin`,"
                                + " `--token-file` or by the `TOKEN` environment variable");
                System.exit(1);
                return;
            }

            Key validationKey;

            if (publicKey != null) {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(publicKey);
                validationKey = AuthTokenUtils.decodePublicKey(encodedKey, algorithm);
            } else {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(secretKey);
                validationKey = AuthTokenUtils.decodeSecretKey(encodedKey);
            }

            // Validate the token
            @SuppressWarnings("unchecked")
            Jwt<?, Claims> jwt = Jwts.parserBuilder()
                    .setSigningKey(validationKey)
                    .build()
                    .parse(token);

            System.out.println(jwt.getBody());
        }
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander(arguments);
        IUsageFormatter usageFormatter = new DefaultUsageFormatter(jcommander);

        CommandCreateSecretKey commandCreateSecretKey = new CommandCreateSecretKey();
        jcommander.addCommand("create-secret-key", commandCreateSecretKey);

        CommandCreateKeyPair commandCreateKeyPair = new CommandCreateKeyPair();
        jcommander.addCommand("create-key-pair", commandCreateKeyPair);

        CommandCreateToken commandCreateToken = new CommandCreateToken();
        jcommander.addCommand("create", commandCreateToken);

        CommandShowToken commandShowToken = new CommandShowToken();
        jcommander.addCommand("show", commandShowToken);

        CommandValidateToken commandValidateToken = new CommandValidateToken();
        jcommander.addCommand("validate", commandValidateToken);

        jcommander.addCommand("gen-doc", new Object());

        try {
            jcommander.parse(args);

            if (arguments.help || jcommander.getParsedCommand() == null) {
                jcommander.usage();
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println(e);
            String chosenCommand = jcommander.getParsedCommand();
            usageFormatter.usage(chosenCommand);
            System.exit(1);
        }

        String cmd = jcommander.getParsedCommand();

        if (cmd.equals("create-secret-key")) {
            commandCreateSecretKey.run();
        } else if (cmd.equals("create-key-pair")) {
            commandCreateKeyPair.run();
        } else if (cmd.equals("create")) {
            commandCreateToken.run();
        } else if (cmd.equals("show")) {
            commandShowToken.run();
        } else if (cmd.equals("validate")) {
            commandValidateToken.run();
        } else if (cmd.equals("gen-doc")) {
            CmdGenerateDocs genDocCmd = new CmdGenerateDocs("pulsar");
            genDocCmd.addCommand("tokens", jcommander);
            genDocCmd.run(null);
        } else {
            System.err.println("Invalid command: " + cmd);
            System.exit(1);
        }
    }
}
