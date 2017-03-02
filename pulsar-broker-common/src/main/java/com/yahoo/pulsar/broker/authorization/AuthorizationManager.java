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
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.cache.ConfigurationCacheService;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.policies.data.AuthAction;

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
    public CompletableFuture<Boolean> canProduceAsync(DestinationName destination, String role) {
        return checkAuthorization(destination, role, AuthAction.produce);
    }
    
    public boolean canProduce(DestinationName destination, String role) throws Exception {
        try {
            return canProduceAsync(destination, role).get();
        } catch (Exception e) {
            log.warn("Producer-client  with Role - {} failed to get permissions for destination - {}", role,
                    destination, e);
            throw e;
        }
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
    public CompletableFuture<Boolean> canConsumeAsync(DestinationName destination, String role) {
        return checkAuthorization(destination, role, AuthAction.consume);
    }
    
    public boolean canConsume(DestinationName destination, String role) throws Exception {
        try {
            return canConsumeAsync(destination, role).get();
        } catch (Exception e) {
            log.warn("Consumer-client  with Role - {} failed to get permissions for destination - {}", role,
                    destination, e);
            throw e;
        }
    }

    /**
     * Check whether the specified role can perform a lookup for the specified destination.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param destination
     * @param role
     * @return
     * @throws Exception 
     */
    public boolean canLookup(DestinationName destination, String role) throws Exception {
        return canProduce(destination, role) || canConsume(destination, role);
    }

    private CompletableFuture<Boolean> checkAuthorization(DestinationName destination, String role,
            AuthAction action) {
        if (isSuperUser(role)) {
            return CompletableFuture.completedFuture(true);
        } else {
            return checkPermission(destination, role, action)
                    .thenApply(isPermission -> isPermission && checkCluster(destination));
        }
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

    public CompletableFuture<Boolean> checkPermission(DestinationName destination, String role,
            AuthAction action) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        try {
            configCache.policiesCache().getAsync(POLICY_ROOT + destination.getNamespace()).thenAccept(policies -> {
                if (!policies.isPresent()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Policies node couldn't be found for destination : {}", destination);
                    }
                    permissionFuture.complete(false);
                } else {
                    Set<AuthAction> namespaceActions = policies.get().auth_policies.namespace_auth.get(role);
                    if (namespaceActions != null && namespaceActions.contains(action)) {
                        // The role has namespace level permission
                        permissionFuture.complete(true);
                    } else {
                        Map<String, Set<AuthAction>> roles = policies.get().auth_policies.destination_auth
                                .get(destination.toString());
                        if (roles == null) {
                            // Destination has no custom policy
                            permissionFuture.complete(false);
                        } else {
                            Set<AuthAction> resourceActions = roles.get(role);
                            if (resourceActions != null && resourceActions.contains(action)) {
                                // The role has destination level permission
                                permissionFuture.complete(true);
                            } else {
                                permissionFuture.complete(false);
                            }
                        }
                    }
                }
            }).exceptionally(ex -> {
                log.warn("Client  with Role - {} failed to get permissions for destination - {}", role, destination,
                        ex);
                permissionFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            log.warn("Client  with Role - {} failed to get permissions for destination - {}", role, destination, e);
            permissionFuture.completeExceptionally(e);
        }
        return permissionFuture;
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
