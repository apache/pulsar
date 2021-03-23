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
package org.apache.pulsar.client.impl.auth.scram;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.SaslConstants;
import org.apache.pulsar.common.sasl.scram.HmacRoleToken;
import org.apache.pulsar.common.sasl.scram.HmacRoleTokenSigner;
import org.apache.pulsar.common.sasl.scram.ScramFormatter;
import org.apache.pulsar.common.sasl.scram.ScramStage;

import javax.naming.AuthenticationException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SaslAuthenticationDataProviderScramImpl implements AuthenticationDataProvider {
    private static final long serialVersionUID = 1L;

    private static final Pattern SERVER_FIRST_MESSAGE = Pattern.compile("r=([^,]*),s=([^,]*),i=(.*)$");

    private static final Pattern SERVER_FINAL_MESSAGE = Pattern.compile("v=([^,]*)$");

    //
    private static final long DEFAULT_SASL_SIGN_TOKEN_EXPIRE_TIME2 = 18000000;

    // 5min
    private static final long DEFAULT_SASL_SIGN_TOKEN_EXPIRE_TIME = 300000;

    private static final String HEADER = "n,,";

    private final String saslRole;

    private final String roleSecret;

    //TCP
    private String clientNonce = null;

    private byte[] saltPassword;

    private String authMessage;

    private ScramStage stage = ScramStage.INIT;

    private transient ScramFormatter sf = null;

    private HmacRoleTokenSigner signer = null;

    public SaslAuthenticationDataProviderScramImpl(String saslRole, String roleSecret) {

        this.saslRole = saslRole;
        this.roleSecret = roleSecret;
        this.sf = new ScramFormatter();
        this.signer = new HmacRoleTokenSigner(roleSecret);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {

        if (Arrays.equals(authData.getBytes(), AuthData.INIT_AUTH_DATA.getBytes())) {
            clientNonce = UUID.randomUUID().toString();
            String clientFirstMessageBare = String.format(Locale.ROOT, "n=%s,r=%s", saslRole, clientNonce);

            String clientFirstMessage = HEADER + clientFirstMessageBare;
            stage = ScramStage.FIRST;
            log.info("SASL-SCRAM-CLIENT INIT (TCP) pulsar client scram auth send client_first_message {}", saslRole);
            return AuthData.of(clientFirstMessage.getBytes(StandardCharsets.UTF_8));
        } else if (ScramStage.FIRST.equals(stage)) {

            try {
                String serverFirstMessage = new String(authData.getBytes(), StandardCharsets.UTF_8);
                Matcher m = SERVER_FIRST_MESSAGE.matcher(serverFirstMessage);
                if (!m.matches()) {
                    stage = ScramStage.FINISH;
                    throw new AuthenticationException(
                            "pulsar client scram auth server_first_message is invalid Matcher failed " + saslRole);
                }

                String nonce = m.group(1);

                if (!nonce.startsWith(clientNonce)) {
                    stage = ScramStage.FINISH;
                    throw new AuthenticationException(
                            "pulsar client scram auth server_first_message is invalid nonce " + saslRole);
                }

                String salt = m.group(2);
                String iterationCountString = m.group(3);
                int iterations = Integer.parseInt(iterationCountString);
                if (iterations <= 0) {
                    stage = ScramStage.FINISH;
                    throw new AuthenticationException(
                            "pulsar client scram auth server_first_message is invalid iterations " + saslRole);
                }

                saltPassword = sf.saltedPassword(roleSecret, Base64.getDecoder().decode(salt), iterations);

                String clientFirstMessageBare = String.format(Locale.ROOT, "n=%s,r=%s", saslRole, clientNonce);

                String clientFinalMessageWithoutProof = "c="
                        + Base64.getEncoder().encodeToString(HEADER.getBytes(StandardCharsets.UTF_8)) + ",r=" + nonce;

                authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

                byte[] clientProof = sf.clientProof(saltPassword, authMessage);

                stage = ScramStage.FINAL;

                String clientFinalMessage = String.format(Locale.ROOT, "%s,p=%s", clientFinalMessageWithoutProof,
                        Base64.getEncoder().encodeToString(clientProof));

                log.info(
                        "SASL-SCRAM-CLIENT FIRST(TCP)  pulsar client scram auth handle server_first_message success and send client_final_message {}",
                        saslRole);
                return AuthData.of(clientFinalMessage.getBytes(StandardCharsets.UTF_8));

            } catch (AuthenticationException aue) {
                throw aue;
            } catch (Exception e) {
                throw new AuthenticationException("ScramStage.FINAL failed");
            }

        } else if (ScramStage.FINAL.equals(stage)) {
            try {
                String serverFinalMessage = new String(authData.getBytes(), StandardCharsets.UTF_8);

                Matcher m = SERVER_FINAL_MESSAGE.matcher(serverFinalMessage);
                if (!m.matches()) {
                    stage = ScramStage.FINISH;
                    throw new AuthenticationException(
                            "SASL-SCRAM-CLIENT FINAL(TCP)  pulsar client scram auth serverFinalMessage is invalid Matcher failed " + saslRole);
                }

                byte[] serverSignature = Base64.getDecoder().decode(m.group(1));

                byte[] serverKey = sf.serverKey(saltPassword);

                byte[] serverSignatureExpected = sf.hmac(serverKey, authMessage.getBytes(StandardCharsets.UTF_8));

                if (!Arrays.equals(serverSignatureExpected, serverSignature)) {
                    throw new AuthenticationException(
                            "pulsar client scram auth server_final_message is invalid, server signature error "
                                    + saslRole);
                }

                stage = ScramStage.FINISH;
                log.info(
                        "SASL-SCRAM-CLIENT FINISH(TCP) pulsar client scram auth handle server_final_message success and send client_complete_message {}",
                        saslRole);

                return AuthData.of("Authentication Complete".getBytes(StandardCharsets.UTF_8));
            } catch (AuthenticationException aue) {
                throw aue;
            } catch (Exception e) {
                throw new AuthenticationException("ScramStage.FINAL failed " + e.getMessage());
            }
        } else {
            throw new AuthenticationException("authData or stage is invalid: " + stage);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Entry<String, String>> getHttpHeaders() throws Exception {
        Map<String, String> headers = new HashMap<>();
        //TODO: token cahce
        try {
            log.info("SASL-SCRAM-CLIENT (HTTP) create sign token {}", saslRole);
            //expired after 10 min
            HmacRoleToken token = new HmacRoleToken(saslRole, "scram", System.currentTimeMillis() + 600000);
            String clientToken = signer.sign(token.toString());

            headers.put(SaslConstants.HMAC_AUTH_ROLE_TOKEN, clientToken);
        } catch (Exception e) {
            log.error("SASL-SCRAM-CLIENT (HTTP) create sign token failed ", e);
        }
        return headers.entrySet();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDataFromCommand() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCommandData() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDataForHttp() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHttpAuthType() {
        return null;
    }

}
