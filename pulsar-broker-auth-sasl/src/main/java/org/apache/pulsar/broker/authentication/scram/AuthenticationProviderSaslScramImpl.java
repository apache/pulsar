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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.scram.HmacRoleToken;
import org.apache.pulsar.common.sasl.scram.HmacRoleTokenSigner;
import org.apache.pulsar.common.sasl.scram.ScramCredential;
import org.apache.pulsar.common.sasl.scram.ScramFormatter;
import org.apache.pulsar.common.util.Decryption;
import org.apache.pulsar.common.util.DecryptionUtils;
import org.apache.zookeeper.ZooKeeper;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.pulsar.common.sasl.SaslConstants.HMAC_AUTH_ROLE_TOKEN;

@Slf4j
public class AuthenticationProviderSaslScramImpl implements AuthenticationProvider {

    private static final String SCRAM_PATH_NAME = "scram-user";

    private CuratorFramework client = null;

    private Map<String, ScramCredential> scramUsers = new ConcurrentHashMap<String, ScramCredential>();

    private Timer userReloadTimer = new Timer("scram-user-reload");

    private Decryption decryptionInterface = null;

    public AuthenticationProviderSaslScramImpl() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(ServiceConfiguration serviceConfiguration) throws IOException {

        log.info("SASL-SCRAM-SERVER INIT sasl Salted Challenge Response Authentication Mechanism(SCRAM) auth");
        initCuratorClient(serviceConfiguration.getZookeeperServers(),
                (int) serviceConfiguration.getZooKeeperSessionTimeoutMillis(),
                (int) serviceConfiguration.getZooKeeperSessionTimeoutMillis());


        decryptionInterface = DecryptionUtils.getDecryptionClass(serviceConfiguration.getDecryptionInterface());

        long reloadTime = Long.parseLong(System.getProperty("pulsar.broker.scram.user.releadTime", "60000"));

//        reload();
        userReloadTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    reload();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 0, reloadTime);


        String adminUser = System.getProperty("pulsarSaslAdminUser");
        String adminPassword = System.getProperty("pulsarSaslAdminPassword");
        String adminSCRAMConfig = System.getProperty("pulsarSaslAdminScramConf");

        if (StringUtils.isNotBlank(adminUser) && StringUtils.isNotBlank(adminPassword) && StringUtils.isNotBlank(adminSCRAMConfig)) {

            String scramsha256 = decryptionInterface.decrypt(adminSCRAMConfig);
            String password = decryptionInterface.decrypt(adminPassword);
            ScramCredential sc = ScramFormatter.credentialFromString(scramsha256);
            sc.setPwd(password);
            scramUsers.put(adminUser, sc);
            log.info("SASL-SCRAM-SERVER init load adminUser from system properties {}", adminUser);
        }
    }


    /**
     *
     */
    @Override
    public AuthenticationState newAuthState(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession)
            throws AuthenticationException {
        log.info("SASL-SCRAM-SERVER New Auth State (TCP)");
        return new SaslAuthenticationStateScramImpl(authData, remoteAddress, sslSession, this);
    }

    /**
     * {@inheritDoc} HTTP auth handle
     */
    @Override
    public String authenticate(AuthenticationDataSource authDataSource) throws AuthenticationException {
        log.info("SASL-SCRAM-SERVER Admin client Auth (HTTP) begin");
        if (!authDataSource.hasDataFromHttp()) {
            log.info("SASL-SCRAM-SERVER Admin client Auth (HTTP) dataSource doesn't has Data For Http");
            throw new AuthenticationException("Authentication failed: not a HTTP request");
        }

        String clientAddress = authDataSource.getPeerAddress().toString();

        String token = authDataSource.getHttpHeader(HMAC_AUTH_ROLE_TOKEN);

        if (token.isEmpty()) {
            log.info("SASL-SCRAM-SERVER Admin client Auth (HTTP) {} token is empty ,clientAddress :{}", HMAC_AUTH_ROLE_TOKEN, clientAddress);
            throw new AuthenticationException(
                    "Authentication datasource does not have a client token message: " + clientAddress);
        }

        int index = token.lastIndexOf(HmacRoleTokenSigner.SIGNATURE);
        if (index == -1) {
            log.info("SASL-SCRAM-SERVER Admin client Auth (HTTP) Invalid format signed text");
            throw new AuthenticationException("Invalid format signed text");
        }

        String originalSignature = token.substring(index + HmacRoleTokenSigner.SIGNATURE.length());
        String rawValue = token.substring(0, index);

        HmacRoleToken hmacRoleToken = HmacRoleToken.parse(rawValue);

        if (hmacRoleToken.isExpired()) {
            log.info("SASL-SCRAM-SERVER Admin client Auth (HTTP) token expired or expire time invalid");
            throw new AuthenticationException("token expired or expire time invalid");
        }

        if (!getScramUsers().containsKey(hmacRoleToken.getUserRole())) {
            throw new AuthenticationException("Invalid user");
        }

        HmacRoleTokenSigner signer = new HmacRoleTokenSigner(getScramUsers().get(hmacRoleToken.getUserRole()).getPwd());

        String currentSignature = signer.computeSignature(rawValue);
        if (!originalSignature.equals(currentSignature)) {
            log.info("SASL-SCRAM-SERVER Admin client Auth (HTTP) Invalid signature");
            throw new AuthenticationException("Invalid signature");
        }
        log.info("SASL-SCRAM-SERVER Admin client Auth (HTTP) Success with user {}", hmacRoleToken.getUserRole());
        return hmacRoleToken.getUserRole();
    }


    @Override
    public boolean authenticateHttpRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {

        if (log.isInfoEnabled()) {
            log.info("SASL-SCRAM-SERVER AuthenticationProvider: SCRAM; authenticateHttpRequest of scram, URI: {}",
                    request == null ? "null" : request.getRequestURI());
        }

        throw new AuthenticationException("current scram not supported");
    }


    /**
     * update user info ,get user info from zookeeper /scram-user
     */
    public void reload() {
        try {

            log.info("SASL-SCRAM-SERVER RELOAD try to reload scram user ");
            List<String> userList = client.getChildren().forPath("/" + SCRAM_PATH_NAME);
            for (String username : userList) {
                byte[] data = client.getData().forPath("/" + SCRAM_PATH_NAME + "/" + username);
                ScramCredential uinfo = getUserScramCredential(username, data);

                if (uinfo == null) {
                    continue;
                }

                if (uinfo.getPwd() == null) {
                    continue;
                }

                if (!getScramUsers().containsKey(username)) {
                    log.info("SASL-SCRAM-SERVER RELOAD load user {}", username);
                    getScramUsers().put(username, uinfo);
                    continue;
                }

                if (!uinfo.getPwd().equals(getScramUsers().get(username).getPwd())) {
                    log.warn("SASL-SCRAM-SERVER RELOAD user : {} password  change !" + username);
                    getScramUsers().put(username, uinfo);
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * init curator zookeeper client
     *
     * @param zkaddr          metadata zookeeper address
     * @param connectTimeout  connect timeout (ms)
     * @param sessionTimerout session timeout (ms)
     * @throws UnsupportedEncodingException
     */
    public void initCuratorClient(String zkaddr, int connectTimeout, int sessionTimerout)
            throws UnsupportedEncodingException {
        client = CuratorFrameworkFactory.builder()
                .connectString(zkaddr)
                .sessionTimeoutMs(sessionTimerout)
                .connectionTimeoutMs(connectTimeout)
                .retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
                .zookeeperFactory((connectString, sessionTimeout, watcher, canBeReadOnly) -> {
                    ZooKeeper zookeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
                    return zookeeper;
                })
                .build();

        client.start();
    }

    /**
     * @param userName
     * @param data
     * @return
     */
    public ScramCredential getUserScramCredential(String userName, byte[] data) {

        try {

            if (data == null) {
                return null;
            }

            String userData = new String(data, "UTF-8");

            ObjectMapper objectMapper = new ObjectMapper();

            ZookeeperSaslScramUser user = objectMapper.readValue(userData, ZookeeperSaslScramUser.class);

            String scramsha256 = decryptionInterface.decrypt(user.getScramSha256());
            String password = decryptionInterface.decrypt(user.getPassword());
            ScramCredential sc = ScramFormatter.credentialFromString(scramsha256);
            sc.setPwd(password);

            return sc;

        } catch (Throwable e) {
            log.error("SASL-SCRAM-SERVER getUserScramCredential error :", e);
        }
        return null;

    }

    public Map<String, ScramCredential> getScramUsers() {
        return scramUsers;
    }

    public void setScramUsers(Map<String, ScramCredential> scramUsers) {
        this.scramUsers = scramUsers;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAuthMethodName() {
        return "scram";
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }

}
