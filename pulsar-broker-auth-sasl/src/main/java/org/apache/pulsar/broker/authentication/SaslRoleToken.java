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
package org.apache.pulsar.broker.authentication;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import javax.naming.AuthenticationException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SaslRoleToken implements Principal {

    /**
     * Constant that identifies an anonymous request.
     */
    public static final SaslRoleToken ANONYMOUS = new SaslRoleToken();

    private static final String ATTR_SEPARATOR = "&";
    private static final String USER_ROLE = "u";
    private static final String EXPIRES = "e";
    private static final String SESSION = "i";

    private static final Set<String> ATTRIBUTES =
        new HashSet<String>(Arrays.asList(USER_ROLE,  EXPIRES,  SESSION));

    private String userRole;
    private String session;
    private long expires;
    private String token;

    private SaslRoleToken() {
        userRole = null;
        session = null;
        expires = -1;
        token = "ANONYMOUS";
        generateToken();
    }

    private static final String ILLEGAL_ARG_MSG = " is NULL, empty or contains a '" + ATTR_SEPARATOR + "'";

    /**
     * Creates an authentication token.
     *
     * @param userRole user name.
     * @param session the sessionId.
     * (<code>System.currentTimeMillis() + validityPeriod</code>).
     */
    public SaslRoleToken(String userRole, String session) {
        checkForIllegalArgument(session, "session");
        this.userRole = userRole;
        this.session = session;
        this.expires = -1;
        generateToken();
    }

    public SaslRoleToken(String userRole, String session, long expires) {
        checkForIllegalArgument(userRole, "userRole");
        checkForIllegalArgument(session, "session");
        this.userRole = userRole;
        this.session = session;
        this.expires = expires;
        generateToken();
    }

    /**
     * Check if the provided value is invalid. Throw an error if it is invalid, NOP otherwise.
     *
     * @param value the value to check.
     * @param name the parameter name to use in an error message if the value is invalid.
     */
    private static void checkForIllegalArgument(String value, String name) {
        if (value == null || value.length() == 0 || value.contains(ATTR_SEPARATOR)) {
            throw new IllegalArgumentException(name + ILLEGAL_ARG_MSG);
        }
    }

    /**
     * Sets the expiration of the token.
     *
     * @param expires expiration time of the token in milliseconds since the epoch.
     */
    public void setExpires(long expires) {
        if (this != SaslRoleToken.ANONYMOUS) {
            this.expires = expires;
            generateToken();
        }
    }

    /**
     * Generates the token.
     */
    private void generateToken() {
        StringBuilder sb = new StringBuilder();
        sb.append(USER_ROLE).append("=").append(getUserRole()).append(ATTR_SEPARATOR);
        sb.append(SESSION).append("=").append(getSession()).append(ATTR_SEPARATOR);
        sb.append(EXPIRES).append("=").append(getExpires());
        token = sb.toString();
    }

    /**
     * Returns the user name.
     *
     * @return the user name.
     */
    public String getUserRole() {
        return userRole;
    }

    /**
     * Returns the principal name (this method name comes from the JDK {@link Principal} interface).
     *
     * @return the principal name.
     */
    @Override
    public String getName() {
        return userRole;
    }

    /**
     * Returns the authentication mechanism of the token.
     *
     * @return the authentication mechanism of the token.
     */
    public String getSession() {
        return session;
    }

    /**
     * Returns the expiration time of the token.
     *
     * @return the expiration time of the token, in milliseconds since Epoc.
     */
    public long getExpires() {
        return expires;
    }

    /**
     * Returns if the token has expired.
     *
     * @return if the token has expired.
     */
    public boolean isExpired() {
        return getExpires() != -1 && System.currentTimeMillis() > getExpires();
    }

    /**
     * Returns the string representation of the token.
     * <p/>
     * This string representation is parseable by the {@link #parse} method.
     *
     * @return the string representation of the token.
     */
    @Override
    public String toString() {
        return token;
    }

    /**
     * Parses a string into an authentication token.
     *
     * @param tokenStr string representation of a token.
     *
     * @return the parsed authentication token.
     *
     * @throws AuthenticationException thrown if the string representation could not be parsed into
     * an authentication token.
     */
    public static SaslRoleToken parse(String tokenStr) throws AuthenticationException {
        Map<String, String> map = split(tokenStr);
        if (!map.keySet().equals(ATTRIBUTES)) {
            throw new AuthenticationException("Invalid token string, missing attributes");
        }
        long expires = Long.parseLong(map.get(EXPIRES));
        SaslRoleToken token = new SaslRoleToken(map.get(USER_ROLE), map.get(SESSION));
        token.setExpires(expires);
        return token;
    }

    /**
     * Splits the string representation of a token into attributes pairs.
     *
     * @param tokenStr string representation of a token.
     *
     * @return a map with the attribute pairs of the token.
     *
     * @throws AuthenticationException thrown if the string representation of the token could not be broken into
     * attribute pairs.
     */
    private static Map<String, String> split(String tokenStr) throws AuthenticationException {
        Map<String, String> map = new HashMap<String, String>();
        StringTokenizer st = new StringTokenizer(tokenStr, ATTR_SEPARATOR);
        while (st.hasMoreTokens()) {
            String part = st.nextToken();
            int separator = part.indexOf('=');
            if (separator == -1) {
                throw new AuthenticationException("Invalid authentication token");
            }
            String key = part.substring(0, separator);
            String value = part.substring(separator + 1);
            map.put(key, value);
        }
        return map;
    }

}
