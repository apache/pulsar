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
package org.apache.pulsar.broker.authorization;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Authorization service that manages pluggable authorization provider and authorize requests accordingly.
 *
 */
public class AuthorizationService {
    private static final Logger log = LoggerFactory.getLogger(AuthorizationService.class);

    private AuthorizationProvider provider;
    private final ServiceConfiguration conf;

    public AuthorizationService(ServiceConfiguration conf, ConfigurationCacheService configCache)
            throws PulsarServerException {

        this.conf = conf;
        if (this.conf.isAuthorizationEnabled()) {
            try {
                final String providerClassname = conf.getAuthorizationProvider();
                if (StringUtils.isNotBlank(providerClassname)) {
                    provider = (AuthorizationProvider) Class.forName(providerClassname).newInstance();
                    provider.initialize(conf, configCache);
                    log.info("{} has been loaded.", providerClassname);
                } else {
                    throw new PulsarServerException("No authorization providers are present.");
                }
            } catch (PulsarServerException e) {
                throw e;
            } catch (Throwable e) {
                throw new PulsarServerException("Failed to load an authorization provider.", e);
            }
        } else {
            log.info("Authorization is disabled");
        }
    }

    public CompletableFuture<Boolean> isSuperUser(String user, AuthenticationDataSource authenticationData) {
        if (provider != null) {
            return provider.isSuperUser(user, authenticationData, conf);
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
    }

    public CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo,
                                                    AuthenticationDataSource authenticationData) {
        if (provider != null) {
            return provider.isTenantAdmin(tenant, role, tenantInfo, authenticationData);
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
    }

    /**
     *
     * Grant authorization-action permission on a namespace to the given client
     *
     * @param namespace
     * @param actions
     * @param role
     * @param authDataJson
     *            additional authdata in json for targeted authorization provider
     * @return
     * @throws IllegalArgumentException
     *             when namespace not found
     * @throws IllegalStateException
     *             when failed to grant permission
     */
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
            String authDataJson) {

        if (provider != null) {
            return provider.grantPermissionAsync(namespace, actions, role, authDataJson);
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
    }

    /**
     * Grant permission to roles that can access subscription-admin api
     *
     * @param namespace
     * @param subscriptionName
     * @param roles
     * @param authDataJson
     *            additional authdata in json for targeted authorization provider
     * @return
     */
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
            Set<String> roles, String authDataJson) {

        if (provider != null) {
            return provider.grantSubscriptionPermissionAsync(namespace, subscriptionName, roles, authDataJson);
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
    }

    /**
     * Revoke subscription admin-api access for a role
     *
     * @param namespace
     * @param subscriptionName
     * @param role
     * @return
     */
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
            String role, String authDataJson) {
        if (provider != null) {
            return provider.revokeSubscriptionPermissionAsync(namespace, subscriptionName, role, authDataJson);
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
    }

    /**
     * Grant authorization-action permission on a topic to the given client
     *
     * @param topicname
     * @param role
     * @param authDataJson
     *            additional authdata in json for targeted authorization provider
     * @return IllegalArgumentException when namespace not found
     * @throws IllegalStateException
     *             when failed to grant permission
     */
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicname, Set<AuthAction> actions, String role,
            String authDataJson) {

        if (provider != null) {
            return provider.grantPermissionAsync(topicname, actions, role, authDataJson);
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));

    }

    /**
     * Check if the specified role has permission to send messages to the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to send messages to the topic.
     */
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {

        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        if (provider != null) {
            return provider.isSuperUser(role, authenticationData, conf).thenComposeAsync(isSuperUser -> {
                if (isSuperUser) {
                    return CompletableFuture.completedFuture(true);
                } else {
                    return provider.canProduceAsync(topicName, role, authenticationData);
                }
            });
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
    }

    /**
     * Check if the specified role has permission to receive messages from the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to receive messages from the topic.
     * @param subscription
     *            the subscription name defined by the client
     */
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData, String subscription) {
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        if (provider != null) {
            return provider.isSuperUser(role, authenticationData, conf).thenComposeAsync(isSuperUser -> {
                if (isSuperUser) {
                    return CompletableFuture.completedFuture(true);
                } else {
                    return provider.canConsumeAsync(topicName, role, authenticationData, subscription);
                }
            });
        }
        return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
    }

    public boolean canProduce(TopicName topicName, String role, AuthenticationDataSource authenticationData)
            throws Exception {
        try {
            return canProduceAsync(topicName, role, authenticationData).get(conf.getZooKeeperOperationTimeoutSeconds(),
                    SECONDS);
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking authorization on {} ", conf.getZooKeeperOperationTimeoutSeconds(),
                    topicName);
            throw e;
        } catch (Exception e) {
            log.warn("Producer-client  with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                    e.getMessage());
            throw e;
        }
    }

    public boolean canConsume(TopicName topicName, String role, AuthenticationDataSource authenticationData,
            String subscription) throws Exception {
        try {
            return canConsumeAsync(topicName, role, authenticationData, subscription)
                    .get(conf.getZooKeeperOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking authorization on {} ", conf.getZooKeeperOperationTimeoutSeconds(),
                    topicName);
            throw e;
        } catch (Exception e) {
            log.warn("Consumer-client  with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                    e.getMessage());
            throw e;
        }
    }

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param topicName
     * @param role
     * @return
     * @throws Exception
     */
    public boolean canLookup(TopicName topicName, String role, AuthenticationDataSource authenticationData)
            throws Exception {
        return canProduce(topicName, role, authenticationData)
                || canConsume(topicName, role, authenticationData, null);
    }

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param topicName
     * @param role
     * @return
     * @throws Exception
     */
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        CompletableFuture<Boolean> finalResult = new CompletableFuture<Boolean>();
        canProduceAsync(topicName, role, authenticationData).whenComplete((produceAuthorized, ex) -> {
            if (ex == null) {
                if (produceAuthorized) {
                    finalResult.complete(produceAuthorized);
                    return;
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Topic [{}] Role [{}] exception occured while trying to check Produce permissions. {}",
                            topicName.toString(), role, ex.getMessage());
                }
            }
            canConsumeAsync(topicName, role, null, null).whenComplete((consumeAuthorized, e) -> {
                if (e == null) {
                    if (consumeAuthorized) {
                        finalResult.complete(consumeAuthorized);
                        return;
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Topic [{}] Role [{}] exception occured while trying to check Consume permissions. {}",
                                topicName.toString(), role, e.getMessage());

                    }
                    finalResult.completeExceptionally(e);
                    return;
                }
                finalResult.complete(false);
            });
        });
        return finalResult;
    }

    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                       AuthenticationDataSource authenticationData) {
        return provider.allowFunctionOpsAsync(namespaceName, role, authenticationData);
    }
}
