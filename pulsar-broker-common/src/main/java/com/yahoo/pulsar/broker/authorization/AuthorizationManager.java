/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.authorization;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.cache.ConfigurationCacheService;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.policies.data.AuthAction;
import com.yahoo.pulsar.common.policies.data.Policies;

/**
 */
public class AuthorizationManager {
    private static final Logger log = LoggerFactory.getLogger(AuthorizationManager.class);

    public final ServiceConfiguration conf;
    public final ConfigurationCacheService configCache;
    private static final String POLICY_ROOT = "/admin/policies/";

    public AuthorizationManager(ServiceConfiguration conf, ConfigurationCacheService configCache) {
        this.conf = conf;
        this.configCache = configCache;
    }

    /**
     * Check if the specified role has permission to send messages to the specified fully qualified destination name.
     *
     * @param destination
     *            the fully qualified destination name associated with the destination.
     * @param role
     *            the app id used to send messages to the destination.
     */
    public boolean canProduce(DestinationName destination, String role) {
        return checkAuthorization(destination, role, AuthAction.produce);
    }

    /**
     * Check if the specified role has permission to receive messages from the specified fully qualified destination
     * name.
     *
     * @param destination
     *            the fully qualified destination name associated with the destination.
     * @param role
     *            the app id used to receive messages from the destination.
     */
    public boolean canConsume(DestinationName destination, String role) {
        return checkAuthorization(destination, role, AuthAction.consume);
    }

    /**
     * Check whether the specified role can perform a lookup for the specified destination.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param destination
     * @param role
     * @return
     */
    public boolean canLookup(DestinationName destination, String role) {
        return canProduce(destination, role) || canConsume(destination, role);
    }

    private boolean checkAuthorization(DestinationName destination, String role, AuthAction action) {
        if (isSuperUser(role))
            return true;
        return checkPermission(destination, role, action) && checkCluster(destination);
    }

    private boolean checkCluster(DestinationName destination) {
        if (destination.isGlobal() || conf.getClusterName().equals(destination.getCluster())) {
            return true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Destination [{}] does not belong to local cluster [{}]", destination.toString(),
                        conf.getClusterName());
            }
            return false;
        }
    }

    public boolean checkPermission(DestinationName destination, String role, AuthAction action) {
        try {
            Optional<Policies> policies = configCache.policiesCache().get(POLICY_ROOT + destination.getNamespace());
            if (!policies.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Policies node couldn't be found for destination : {}", destination);
                }
                return false;
            }

            Set<AuthAction> namespaceActions = policies.get().auth_policies.namespace_auth.get(role);
            if (namespaceActions != null && namespaceActions.contains(action)) {
                // The role has namespace level permission
                return true;
            }

            Map<String, Set<AuthAction>> roles = policies.get().auth_policies.destination_auth.get(destination.toString());
            if (roles == null) {
                // Destination has no custom policy
                return false;
            }

            Set<AuthAction> resourceActions = roles.get(role);
            if (resourceActions != null && resourceActions.contains(action)) {
                // The role has destination level permission
                return true;
            }
            return false;
        } catch (Exception e) {
            log.warn("Client  with Role - {} failed to get permissions for destination - {}", role, destination, e);
            return false;
        }
    }

    /**
     * Super user roles are allowed to do anything, used for replication primarily
     *
     * @param role
     *            the app id used to receive messages from the destination.
     */
    public boolean isSuperUser(String role) {
        Set<String> superUserRoles = conf.getSuperUserRoles();
        return role != null && superUserRoles.contains(role) ? true : false;
    }

}
