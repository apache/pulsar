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
package org.apache.pulsar.io.hdfs3;

import java.io.IOException;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Provides synchronized access to UserGroupInformation to avoid multiple processors/services from
 * interfering with each other.
 */
public class SecurityUtil {
    public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static final String KERBEROS = "kerberos";

    /**
     *  Initializes UserGroupInformation with the given Configuration and performs the login for the
     *  given principal and keytab. All logins should happen through this class to ensure other threads
     *  are not concurrently modifying UserGroupInformation.
     * <p/>
     * @param config the configuration instance
     * @param principal the principal to authenticate as
     * @param keyTab the keytab to authenticate with
     *
     * @return the UGI for the given principal
     *
     * @throws IOException if login failed
     */
    public static synchronized UserGroupInformation loginKerberos(final Configuration config,
            final String principal, final String keyTab) throws IOException {
        Validate.notNull(config);
        Validate.notNull(principal);
        Validate.notNull(keyTab);

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab(principal.trim(), keyTab.trim());
        return UserGroupInformation.getCurrentUser();
    }

    /**
     * Initializes UserGroupInformation with the given Configuration and
     * returns UserGroupInformation.getLoginUser(). All logins should happen
     * through this class to ensure other threads are not concurrently
     * modifying UserGroupInformation.
     *
     * @param config the configuration instance
     *
     * @return the UGI for the given principal
     *
     * @throws IOException if login failed
     */
    public static synchronized UserGroupInformation loginSimple(final Configuration config) throws IOException {
        Validate.notNull(config);
        UserGroupInformation.setConfiguration(config);
        return UserGroupInformation.getLoginUser();
    }

    /**
     * Initializes UserGroupInformation with the given Configuration and returns
     * UserGroupInformation.isSecurityEnabled().
     * All checks for isSecurityEnabled() should happen through this method.
     *
     * @param config the given configuration
     *
     * @return true if kerberos is enabled on the given configuration, false otherwise
     *
     */
    public static boolean isSecurityEnabled(final Configuration config) {
        Validate.notNull(config);
        return KERBEROS.equalsIgnoreCase(config.get(HADOOP_SECURITY_AUTHENTICATION));
    }
}
