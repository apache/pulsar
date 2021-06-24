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
package org.apache.pulsar.broker.authz.ranger;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.RestException;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RangerAuthorizationProvider implements AuthorizationProvider {

    private static final Logger log = LoggerFactory.getLogger(RangerAuthorizationProvider.class);

    public static final String KEY_TENANT = "tenant";
    public static final String KEY_NAMESPACE = "namespace";
    public static final String KEY_TOPIC = "topic";
    public static final String KEY_TAG = "tid";
    public static final String KEY_SUBSCRIPTION = "subscription";
    public static final String KEY_CONSUMER_GROUP = "consumer_group";

    private static final String ARGS_SERVICE_TARGET = "rangerServiceTarget";
    private static final String ARGS_APP_ID = "appId";

    private String serviceTarget = "pulsar";
    private String appId = "pulsar";
    private RangerBasePlugin rangerPlugin;
    private ServiceConfiguration conf;
    private ConfigurationCacheService configCache;



    @Override
    public void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache)
            throws IOException {
        Preconditions.checkNotNull(conf, "ServiceConfiguration can't be null");
        Preconditions.checkNotNull(configCache, "ConfigurationCacheService can't be null");

        if (conf.getProperty(ARGS_SERVICE_TARGET) != null
                && StringUtils.isNotBlank((String) conf.getProperty(ARGS_SERVICE_TARGET))) {
            log.info("authServiceTarget is {}", conf.getProperty(ARGS_SERVICE_TARGET));
            serviceTarget = (String) conf.getProperty(ARGS_SERVICE_TARGET);
        }

        if (conf.getProperty(ARGS_APP_ID) != null
                && StringUtils.isNotBlank((String) conf.getProperty(ARGS_APP_ID))) {
            log.info("appId is {}", conf.getProperty(ARGS_APP_ID));
            appId = (String) conf.getProperty(ARGS_APP_ID);
        }

        log.info("serviceTarget {}, appId {}", serviceTarget, appId);
        this.conf = conf;
        this.configCache = configCache;
        this.rangerPlugin = new RangerBasePlugin(serviceTarget, appId);
        this.rangerPlugin.init();
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(KEY_TENANT, topicName.getTenant());
        resource.setValue(KEY_NAMESPACE, topicName.getNamespacePortion());
        resource.setValue(KEY_TOPIC, topicName.getLocalName().split("-partition-")[0]);
        resource.setValue(KEY_TAG, "*");

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();

        request.setAccessType(AuthAction.produce.name());
        request.setUser(role);
        request.setResource(resource);

        try {
            RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
            if (result.getIsAllowed()) {
                future.complete(true);
            } else {
                String errMsg = String
                        .format("User '%s' doesn't have produce access to %s, matched policy id = %d",
                                request.getUser(), topicName.toString(), result.getPolicyId());
                log.error(errMsg);
                future.completeExceptionally(new Exception(errMsg));
            }
        } catch (Exception e) {
            // access allowed in abnormal situation
            log.error("User {} encounter exception in {} produce authorization step.",
                    request.getUser(), topicName.toString(), e);
            future.complete(true);
        }

        return future;
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData, String subscription) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();



        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(KEY_TENANT, topicName.getTenant());
        resource.setValue(KEY_NAMESPACE, topicName.getNamespacePortion());
        resource.setValue(KEY_TOPIC, topicName.getLocalName().split("-partition-")[0]);
        resource.setValue(KEY_TAG, "*");
//        resource.setValue(KEY_SUBSCRIPTION, subscription);

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setAccessType(AuthAction.consume.name());
        request.setUser(role);

        if (subscription != null) {
            request.getContext().put(KEY_CONSUMER_GROUP, subscription);
        }
        request.setResource(resource);

        try {
            RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
            if (result.getIsAllowed()) {
                future.complete(true);
            } else {
                String errMsg = String
                        .format("User '%s' doesn't have consume access to %s with subscription %s, matched policy id = %d",
                                request.getUser(), topicName.toString(), subscription,
                                result.getPolicyId());
                log.error(errMsg);
                future.completeExceptionally(new Exception(errMsg));
            }
        } catch (Exception e) {
            // access allowed in abnormal situation
            log.error("User {} encounter exception in {} consume authorization step.",
                    request.getUser(), topicName.toString(), e);
            future.complete(true);
        }

        return future;
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        CompletableFuture<Boolean> finalResult = new CompletableFuture<Boolean>();
        canProduceAsync(topicName, role, authenticationData)
                .whenComplete((produceAuthorized, ex) -> {
                    if (ex == null) {
                        if (produceAuthorized) {
                            finalResult.complete(true);
                            return;
                        }
                    } else {
                        log.error("Topic [{}] Role [{}] exception occurred while trying to check Produce permissions. {}",
                                topicName.toString(), role, ex.getMessage());
                    }
                    canConsumeAsync(topicName, role, authenticationData, null)
                            .whenComplete((consumeAuthorized, e) -> {
                                if (e == null) {
                                    if (consumeAuthorized) {
                                        finalResult.complete(true);
                                        return;
                                    }
                                } else {
                                    log.error("Topic [{}] Role [{}] exception occurred while trying to check Consume permissions. {}",
                                            topicName.toString(), role, e.getMessage());

                                    String errMsg = String.format("User '%s' doesn't have lookup access to %s ",
                                            role, topicName.toString());
                                    finalResult.completeExceptionally(new Exception(errMsg));
                                    return;
                                }
                                finalResult.complete(false);
                            });
                });
        return finalResult;
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName,
            String role, AuthenticationDataSource authenticationData) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(KEY_TENANT, namespaceName.getTenant());
        resource.setValue(KEY_NAMESPACE, namespaceName.getLocalName());

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setAccessType(AuthAction.functions.name());
        request.setUser(role);
        request.setResource(resource);

        try {
            RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
            if (result.getIsAllowed()) {
                future.complete(true);
            } else {
                String errMsg = String
                        .format("User '%s' doesn't have function access to %s, matched policy id = %d",
                                request.getUser(), namespaceName.toString(), result.getPolicyId());
                log.error(errMsg);
                future.completeExceptionally(new Exception(errMsg));
            }
        } catch (Exception e) {
            // access allowed in abnormal situation
            log.error("User {} encounter exception in {} function authorization step.",
                    request.getUser(), namespaceName.toString(), e);
            future.complete(true);
        }

        return future;
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName,
            String role, AuthenticationDataSource authenticationData) {
        return FutureUtil.failedFuture(new IllegalAccessException("not implement"));
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName,
            String role, AuthenticationDataSource authenticationData) {
        return FutureUtil.failedFuture(new IllegalAccessException("not implement"));
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace,
            Set<AuthAction> actions, String role, String authDataJson) {
        return FutureUtil.failedFuture(
                new IllegalAccessException("not allowed to grant permission at ranger plugin"));
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace,
            String subscriptionName, Set<String> roles, String authDataJson) {
        return FutureUtil.failedFuture(
                new IllegalAccessException("not allowed to grant permission at ranger plugin"));
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace,
            String subscriptionName, String role, String authDataJson) {
        return FutureUtil.failedFuture(
                new IllegalAccessException("not allowed to revoke permission at ranger plugin"));
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName,
            Set<AuthAction> actions, String role, String authDataJson) {
        return FutureUtil.failedFuture(
                new IllegalAccessException("not allowed to grant permission at ranger plugin"));
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName,
            String role, TopicOperation operation, AuthenticationDataSource authData) {
        CompletableFuture<Boolean> isAuthorizedFuture;

        switch (operation) {
            case LOOKUP: isAuthorizedFuture = canLookupAsync(topicName, role, authData);
                break;
            case PRODUCE: isAuthorizedFuture = canProduceAsync(topicName, role, authData);
                break;
            case CONSUME: isAuthorizedFuture = canConsumeAsync(topicName, role, authData, authData.getSubscription());
                break;
            default: isAuthorizedFuture = FutureUtil.failedFuture(
                    new IllegalStateException("Topic Operation is not supported."));
        }

        CompletableFuture<Boolean> isSuperUserFuture = isSuperUser(role, authData, conf);

        return isSuperUserFuture
                .thenCombine(isAuthorizedFuture, (isSuperUser, isAuthorized) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Verify if role {} is allowed to {} to topic {}:"
                                        + " isSuperUser={}, isAuthorized={}",
                                role, operation, topicName, isSuperUser, isAuthorized);
                    }
                    return isSuperUser || isAuthorized;
                });
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName,
            String role, TenantOperation operation, AuthenticationDataSource authData) {
        return validateTenantAdminAccess(tenantName, role, authData);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
            String role, NamespaceOperation operation, AuthenticationDataSource authData) {
        return validateTenantAdminAccess(namespaceName.getTenant(), role, authData);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
            PolicyName policy, PolicyOperation operation, String role, AuthenticationDataSource authData) {
        return validateTenantAdminAccess(namespaceName.getTenant(), role, authData);
    }

    private static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("/admin/");
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    private CompletableFuture<Boolean> validateTenantAdminAccess(String tenantName,
            String role,
            AuthenticationDataSource authData) {
        try {
            TenantInfo tenantInfo = configCache.propertiesCache()
                    .get(path(POLICIES, tenantName))
                    .orElseThrow(() -> new RestException(Response.Status.NOT_FOUND, "Tenant does not exist"));

            // role check
            CompletableFuture<Boolean> isRoleSuperUserFuture = isSuperUser(role, authData, conf);
            CompletableFuture<Boolean> isRoleTenantAdminFuture = isTenantAdmin(tenantName, role, tenantInfo, authData);
            return isRoleSuperUserFuture
                    .thenCombine(isRoleTenantAdminFuture, (isRoleSuperUser, isRoleTenantAdmin) ->
                            isRoleSuperUser || isRoleTenantAdmin);
        } catch (KeeperException.NoNodeException e) {
            log.warn("Failed to get tenant info data for non existing tenant {}", tenantName);
            throw new RestException(Response.Status.NOT_FOUND, "Tenant does not exist");
        } catch (Exception e) {
            log.error("Failed to get tenant {}", tenantName, e);
            throw new RestException(e);
        }
    }

    @Override
    public void close() throws IOException {
        rangerPlugin.cleanup();
    }
}
