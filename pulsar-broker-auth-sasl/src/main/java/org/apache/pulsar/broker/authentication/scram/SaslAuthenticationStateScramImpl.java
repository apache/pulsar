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
package org.apache.pulsar.broker.authentication.scram;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.scram.ScramFormatter;
import org.apache.pulsar.common.sasl.scram.ScramStage;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;


@Slf4j
public class SaslAuthenticationStateScramImpl implements AuthenticationState {

    private static final Pattern CLIENT_FIRST_MESSAGE =
            Pattern.compile("^(([pny])=?([^,]*),([^,]*),)(m?=?[^,]*,?n=([^,]*),r=([^,]*),?.*)$");

    private static final Pattern CLIENT_FINAL_MESSAGE = Pattern.compile("(c=([^,]*),r=([^,]*)),p=(.*)$");

    private final String mServerPartNonce = UUID.randomUUID().toString();

    private final AuthenticationDataSource authenticationDataSource;

    private final long stateId;

    private String credential;

    private String clientFirstMessageBare;

    private String mixNonce;

    private String serverFirstMessage;

    private ScramStage stage = ScramStage.INIT;

    private AuthenticationProviderSaslScramImpl provider;

    private transient ScramFormatter sf = new ScramFormatter();

    public SaslAuthenticationStateScramImpl(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession,
                                            AuthenticationProviderSaslScramImpl provider) throws AuthenticationException {

        log.info("SASL-SCRAM-SERVER TCP-STATE constructed");

        this.provider = provider;
        this.authenticationDataSource =
                new SaslAuthenticationDataSourceScramImpl(new String(authData.getBytes(), UTF_8), remoteAddress, sslSession);
        stateId = new SecureRandom().nextLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {

        String clientAddress;

        if (!authenticationDataSource.hasDataFromPeer()) {
            throw new AuthenticationException("pulsar server scrame auth hasDataFromPeer return false ");
        }

        SocketAddress socketAddress = authenticationDataSource.getPeerAddress();
        clientAddress = socketAddress.toString();

        if (ScramStage.INIT.equals(stage)) {

            if (authData == null) {
                throw new AuthenticationException(
                        "pulsar server scrame auth does not have a client message, from client: " + clientAddress);
            }

            String clientFirstMessage = new String(authData.getBytes(), UTF_8);

            if (clientFirstMessage.isEmpty()) {
                throw new AuthenticationException(
                        "pulsar server scrame auth client_first_message is empty, from client: " + clientAddress);
            }

            Matcher m = CLIENT_FIRST_MESSAGE.matcher(clientFirstMessage);
            if (!m.matches()) {
                throw new AuthenticationException(
                        "pulsar server scrame auth client_first_message is invalid, format error " + clientAddress);
            }

            // sample n,,n=user,r=xxxxx
            clientFirstMessageBare = m.group(5);
            credential = m.group(6);
            String clientNonce = m.group(7);

            String salt = Base64.getEncoder().encodeToString(getSaltByUser(credential));
            int iteration = getIterationByUser(credential);
            mixNonce = clientNonce + mServerPartNonce;
            serverFirstMessage = String.format(Locale.ROOT, "r=%s,s=%s,i=%d", mixNonce, salt, iteration);

            stage = ScramStage.FIRST;

            log.info(
                    "SASL-SCRAM-SERVER TCP-STATE AUTH INIT pulsar server scram auth handler client_first_message success and return server_first_message {}",
                    credential);

            return AuthData.of(serverFirstMessage.getBytes(StandardCharsets.UTF_8));

        } else if (ScramStage.FIRST.equals(stage)) {

            try {
                if (authData == null) {
                    throw new AuthenticationException(
                            "pulsar server scrame auth does not have a client_final_Message, from client: " + credential);
                }

                String clientFinalMessage = new String(authData.getBytes(), UTF_8);

                Matcher m = CLIENT_FINAL_MESSAGE.matcher(clientFinalMessage);
                if (!m.matches()) {
                    stage = ScramStage.FINISH;
                    throw new AuthenticationException(
                            "pulsar server scrame auth  client_final_Message invalid, from client: " + credential);
                }

                String clientFinalMessageWithoutProof = m.group(1);
                String clientNonce = m.group(3);
                String proof = m.group(4);

                if (!mixNonce.equals(clientNonce)) {
                    stage = ScramStage.FINISH;
                    throw new AuthenticationException(
                            "pulsar server scrame auth  client_final_Message  mixNonce not equal, from client: "
                                    + credential);
                }

                String authMessage =
                        clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

                byte[] expectedStoredKey = getStoredKeyByUser(credential);
                byte[] clientSignature = sf.hmac(expectedStoredKey, authMessage.getBytes(StandardCharsets.UTF_8));

                byte[] decodedProof = Base64.getDecoder().decode(proof);

                byte[] computedStoredKey = sf.storedKey(clientSignature, decodedProof);

                if (!Arrays.equals(computedStoredKey, expectedStoredKey)) {
                    throw new AuthenticationException(
                            "pulsar server scrame auth  client_final_Message  is invalid, storekey authentication failed"
                                    + credential);
                }

                byte[] serverKey = getServerKeyByUser(credential);
                byte[] serverSignature = sf.hmac(serverKey, authMessage.getBytes(StandardCharsets.UTF_8));

                String serverFinalMessage = "v=" + Base64.getEncoder().encodeToString(serverSignature);
                stage = ScramStage.FINAL;

                log.info(
                        "SASL-SCRAM-SERVER TCP-STATE AUTH FIRST pulsar server scrame auth  client_final_Message handle success and return server_final_message : {}",
                        credential);

                return AuthData.of(serverFinalMessage.getBytes(StandardCharsets.UTF_8));

            } catch (AuthenticationException aue) {
                throw aue;
            } catch (Exception e) {
                throw new AuthenticationException("ScramStage.FIRST failed");
            }

        } else if (ScramStage.FINAL.equals(stage)) {
            log.info("SASL-SCRAM-SERVER TCP-STATE AUTH FINISH success");
            stage = ScramStage.FINISH;
            return null;
        } else {
            log.error("SASL-SCRAM-SERVER pulsar server scrame auth error with invalid stage");
            throw new AuthenticationException(
                    "Authentication datasource stage is invalid: " + stage + ", from client: " + credential);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isComplete() {
        boolean isFinish = ScramStage.FINISH.equals(stage);
        return isFinish;
    }

    private byte[] getSaltByUser(String credential) {
        if (!provider.getScramUsers().containsKey(credential)) {
            throw new IllegalArgumentException();
        }
        return provider.getScramUsers().get(credential).salt();
    }

    private int getIterationByUser(String credential) {
        if (!provider.getScramUsers().containsKey(credential)) {
            throw new IllegalArgumentException();
        }
        return provider.getScramUsers().get(credential).iterations();
    }

    private byte[] getStoredKeyByUser(String credential) {

        if (!provider.getScramUsers().containsKey(credential)) {
            throw new IllegalArgumentException();
        }
        return provider.getScramUsers().get(credential).storedKey();

    }

    private byte[] getServerKeyByUser(String credential) {
        if (!provider.getScramUsers().containsKey(credential)) {
            throw new IllegalArgumentException();
        }
        return provider.getScramUsers().get(credential).serverKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AuthenticationDataSource getAuthDataSource() {
        return authenticationDataSource;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAuthRole() {
        return credential;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getStateId() {
        return stateId;
    }

}
