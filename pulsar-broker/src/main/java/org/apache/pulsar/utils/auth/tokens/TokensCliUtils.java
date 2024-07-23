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
package org.apache.pulsar.utils.auth.tokens;

import com.google.common.annotations.VisibleForTesting;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.io.Encoders;
import io.jsonwebtoken.security.Keys;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyPair;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToMillisConverter;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;

@Command(name = "tokens", showDefaultValues = true, scope = ScopeType.INHERIT)
public class TokensCliUtils {

    private final CommandLine commander;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message")
    private boolean help;

    @Command(description = "Create a new secret key")
    public static class CommandCreateSecretKey implements Callable<Integer> {
        @Option(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the new secret key.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.HS256;

        @Option(names = {"-o",
                "--output"}, description = "Write the secret key to a file instead of stdout")
        String outputFile;

        @Option(names = {
                "-b", "--base64"}, description = "Encode the key in base64")
        boolean base64 = false;

        @Override
        public Integer call() throws Exception {
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

            return 0;
        }
    }

    @Command(description = "Create a new or pair of keys public/private")
    public static class CommandCreateKeyPair implements Callable<Integer> {
        @Option(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the new key pair.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

        @Option(names = {
                "--output-private-key"}, description = "File where to write the private key", required = true)
        String privateKeyFile;
        @Option(names = {
                "--output-public-key"}, description = "File where to write the public key", required = true)
        String publicKeyFile;

        @Override
        public Integer call() throws Exception {
            KeyPair pair = Keys.keyPairFor(algorithm);

            Files.write(Paths.get(publicKeyFile), pair.getPublic().getEncoded());
            Files.write(Paths.get(privateKeyFile), pair.getPrivate().getEncoded());

            return 0;
        }
    }

    @Command(description = "Create a new token")
    public static class CommandCreateToken implements Callable<Integer> {
        @Option(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the new key pair.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

        @Option(names = {"-s",
                "--subject"},
                description = "Specify the 'subject' or 'principal' associate with this token", required = true)
        private String subject;

        @Option(names = {"-e",
                "--expiry-time"},
                description = "Relative expiry time for the token (eg: 1h, 3d, 10y)."
                        + " (m=minutes) Default: no expiration",
                converter = TimeUnitToMillisConverter.class)
        private Long expiryTime = null;

        @Option(names = {"-sk",
                "--secret-key"},
                description = "Pass the secret key for signing the token. This can either be: data:, file:, etc..")
        private String secretKey;

        @Option(names = {"-pk",
                "--private-key"},
                description = "Pass the private key for signing the token. This can either be: data:, file:, etc..")
        private String privateKey;

        @Option(names = {"-hs",
                "--headers"},
                description = "Additional headers to token. Format: --headers key1=value1")
        private Map<String, Object> headers;

        @Override
        public Integer call() throws Exception {
            if (secretKey == null && privateKey == null) {
                System.err.println(
                        "Either --secret-key or --private-key needs to be passed for signing a token");
                return 1;
            } else if (secretKey != null && privateKey != null) {
                System.err.println(
                        "Only one of --secret-key and --private-key needs to be passed for signing a token");
                return 1;
            }

            Key signingKey;

            if (privateKey != null) {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(privateKey);
                signingKey = AuthTokenUtils.decodePrivateKey(encodedKey, algorithm);
            } else {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(secretKey);
                signingKey = AuthTokenUtils.decodeSecretKey(encodedKey);
            }

            Optional<Date> optExpiryTime = (expiryTime == null)
                    ? Optional.empty()
                    : Optional.of(new Date(System.currentTimeMillis() + expiryTime));

            String token = AuthTokenUtils.createToken(signingKey, subject, optExpiryTime, Optional.ofNullable(headers));
            System.out.println(token);

            return 0;
        }
    }

    @Command(description = "Show the content of token")
    public static class CommandShowToken implements Callable<Integer> {

        @Parameters(description = "The token string", arity = "0..1")
        private String args;

        @Option(names = {"-i",
                "--stdin"}, description = "Read token from standard input")
        private Boolean stdin = false;

        @Option(names = {"-f",
                "--token-file"}, description = "Read token from a file")
        private String tokenFile;

        @Override
        public Integer call() throws Exception {
            String token;
            if (args != null) {
                token = args;
            } else if (stdin) {
                @Cleanup
                BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
                token = r.readLine();
            } else if (tokenFile != null) {
                token = new String(Files.readAllBytes(Paths.get(tokenFile)), StandardCharsets.UTF_8);
            } else if (System.getenv("TOKEN") != null) {
                token = System.getenv("TOKEN");
            } else {
                System.err.println(
                        "Token needs to be either passed as an argument or through `--stdin`,"
                                + " `--token-file` or by the `TOKEN` environment variable");
                return 1;
            }

            String[] parts = token.split("\\.");
            System.out.println(new String(Decoders.BASE64URL.decode(parts[0])));
            System.out.println("---");
            System.out.println(new String(Decoders.BASE64URL.decode(parts[1])));

            return 0;
        }
    }

    @Command(description = "Validate a token against a key")
    public static class CommandValidateToken implements Callable<Integer> {

        @Option(names = {"-a",
                "--signature-algorithm"}, description = "The signature algorithm for the key pair if using public key.")
        SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

        @Parameters(description = "The token string", arity = "0..1")
        private String args;

        @Option(names = {"-i",
                "--stdin"}, description = "Read token from standard input")
        private Boolean stdin = false;

        @Option(names = {"-f",
                "--token-file"}, description = "Read token from a file")
        private String tokenFile;

        @Option(names = {"-sk",
                "--secret-key"},
                description = "Pass the secret key for validating the token. This can either be: data:, file:, etc..")
        private String secretKey;

        @Option(names = {"-pk",
                "--public-key"},
                description = "Pass the public key for validating the token. This can either be: data:, file:, etc..")
        private String publicKey;

        @Override
        public Integer call() throws Exception {
            if (secretKey == null && publicKey == null) {
                System.err.println(
                        "Either --secret-key or --public-key needs to be passed for signing a token");
                return 1;
            } else if (secretKey != null && publicKey != null) {
                System.err.println(
                        "Only one of --secret-key and --public-key needs to be passed for signing a token");
                return 1;
            }

            String token;
            if (args != null) {
                token = args;
            } else if (stdin) {
                @Cleanup
                BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
                token = r.readLine();
            } else if (tokenFile != null) {
                token = new String(Files.readAllBytes(Paths.get(tokenFile)), StandardCharsets.UTF_8);
            } else if (System.getenv("TOKEN") != null) {
                token = System.getenv("TOKEN");
            } else {
                System.err.println(
                        "Token needs to be either passed as an argument or through `--stdin`,"
                                + " `--token-file` or by the `TOKEN` environment variable");
                return 1;
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
            Jwt<?, Claims> jwt = Jwts.parserBuilder()
                    .setSigningKey(validationKey)
                    .build()
                    .parseClaimsJws(token);

            System.out.println(jwt.getBody());
            return 0;
        }
    }

    @Command
    static class GenDoc implements Callable<Integer> {

        private final CommandLine rootCmd;

        public GenDoc(CommandLine rootCmd) {
            this.rootCmd = rootCmd;
        }

        @Override
        public Integer call() throws Exception {
            CmdGenerateDocs genDocCmd = new CmdGenerateDocs("pulsar");
            genDocCmd.addCommand("tokens", rootCmd);
            genDocCmd.run(null);

            return 0;
        }
    }

    TokensCliUtils() {
        commander = new CommandLine(this);
        commander.addSubcommand("create-secret-key", CommandCreateSecretKey.class);
        commander.addSubcommand("create-key-pair", CommandCreateKeyPair.class);
        commander.addSubcommand("create", CommandCreateToken.class);
        commander.addSubcommand("show", CommandShowToken.class);
        commander.addSubcommand("validate", CommandValidateToken.class);
        commander.addSubcommand("gen-doc", new GenDoc(commander));
    }

    @VisibleForTesting
    int execute(String[] args) {
        return commander.execute(args);
    }

    public static void main(String[] args) throws Exception {
        TokensCliUtils tokensCliUtils = new TokensCliUtils();
        System.exit(tokensCliUtils.execute(args));
    }
}
