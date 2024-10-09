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
package org.apache.pulsar.broker.authorization;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.resources.PulsarResources;
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
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * Provider of authorization mechanism.
 */
public interface AuthorizationProvider extends Closeable {

    /**
     * Check if specified role is a super user.
     * @param role the role to check
     * @param authenticationData authentication data related to the role
     * @return a CompletableFuture containing a boolean in which true means the role is a super user
     * and false if it is not
     */
    default CompletableFuture<Boolean> isSuperUser(String role,
                                                   AuthenticationDataSource authenticationData,
                                                   ServiceConfiguration serviceConfiguration) {
        Set<String> superUserRoles = serviceConfiguration.getSuperUserRoles();
        return CompletableFuture.completedFuture(role != null && superUserRoles.contains(role));
    }

    /**
     * Check if specified role is an admin of the tenant.
     * @param tenant the tenant to check
     * @param role the role to check
     * @return a CompletableFuture containing a boolean in which true means the role is an admin user
     * and false if it is not
     */
    default CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo,
                                                     AuthenticationDataSource authenticationData) {
        return CompletableFuture.completedFuture(role != null && tenantInfo.getAdminRoles() != null
                && tenantInfo.getAdminRoles().contains(role));
    }

    /**
     * Perform initialization for the authorization provider.
     *
     * @param conf
     *            broker config object
     * @param pulsarResources
     *            Resources component for access to metadata
     * @throws IOException
     *             if the initialization fails
     */
    default void initialize(ServiceConfiguration conf, PulsarResources pulsarResources) throws IOException {
    }

    /**
     * Check if the specified role has permission to send messages to the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to send messages to the topic.
     */
    CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData);

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
    CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData, String subscription);

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
    CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData);

    /**
     * Allow all function operations with in this namespace.
     * @param namespaceName The namespace that the function operations can be executed in
     * @param role The role to check
     * @param authenticationData authentication data related to the role
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                     AuthenticationDataSource authenticationData);

    /**
     * Allow all source operations with in this namespace.
     * @param namespaceName The namespace that the sources operations can be executed in
     * @param role The role to check
     * @param authenticationData authentication data related to the role
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role,
                                                   AuthenticationDataSource authenticationData);

    /**
     * Allow all sink operations with in this namespace.
     * @param namespaceName The namespace that the sink operations can be executed in
     * @param role The role to check
     * @param authenticationData authentication data related to the role
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role,
                                                 AuthenticationDataSource authenticationData);

    /**
     *
     * Grant authorization-action permission on a namespace to the given client.
     *
     * NOTE: used to complete with {@link IllegalArgumentException} when namespace not found or with
     * {@link IllegalStateException} when failed to grant permission. This behavior is now deprecated.
     * Please use the appropriate {@link MetadataStoreException}.
     *
     * @param namespace
     * @param actions
     * @param role
     * @param authDataJson
     *            additional authdata in json format
     * @return CompletableFuture
     * @completesWith null once the permissions are updated successfully.
     * @completesWith {@link MetadataStoreException} when the MetadataStore is not updated.
     */
    CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
            String authDataJson);

    /**
     * Revoke authorization-action permission on a namespace to the given client.
     * @param namespace
     * @param role
     * @return CompletableFuture<Void>
     */
    default CompletableFuture<Void> revokePermissionAsync(NamespaceName namespace, String role) {
        return FutureUtil.failedFuture(new IllegalStateException(
                String.format("revokePermissionAsync on namespace %s is not supported by the Authorization",
                        namespace)));
    }

    /**
     * Grant permission to roles that can access subscription-admin api.
     *
     * @param namespace
     * @param subscriptionName
     * @param roles
     * @param authDataJson
     *            additional authdata in json format
     * @return CompletableFuture<Void>
     */
    CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                             Set<String> roles, String authDataJson);

    /**
     * Revoke subscription admin-api access for a role.
     * @param namespace
     * @param subscriptionName
     * @param role
     * @return CompletableFuture<Void>
     */
    CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
            String role, String authDataJson);

    /**
     * Grant authorization-action permission on a topic to the given client.
     *
     * NOTE: used to complete with {@link IllegalArgumentException} when namespace not found or with
     * {@link IllegalStateException} when failed to grant permission. This behavior is now deprecated.
     * Please use the appropriate {@link MetadataStoreException}.
     *
     * @param topicName
     * @param role
     * @param authDataJson
     *            additional authdata in json format
     * @return CompletableFuture
     * @completesWith null once the permissions are updated successfully.
     * @completesWith {@link MetadataStoreException} when the MetadataStore is not updated.
     */
    CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role,
            String authDataJson);


    /**
     * Revoke authorization-action permission on a topic to the given client.
     * @param topicName
     * @param role
     * @return CompletableFuture<Void>
     */
    default CompletableFuture<Void> revokePermissionAsync(TopicName topicName, String role) {
        return FutureUtil.failedFuture(new IllegalStateException(
                String.format("revokePermissionAsync on topicName %s is not supported by the Authorization",
                        topicName)));
    }

    /**
     * Check if a given <tt>role</tt> is allowed to execute a given <tt>operation</tt> on the tenant.
     *
     * @param tenantName tenant name
     * @param role role name
     * @param operation tenant operation
     * @param authData authenticated data of the role
     * @return a completable future represents check result
     */
    default CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String role,
                                                                 TenantOperation operation,
                                                                 AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(new IllegalStateException(
            String.format("allowTenantOperation(%s) on tenant %s is not supported by the Authorization"
                            + " provider you are using.",
                operation.toString(), tenantName)));
    }

    /**
     * Check if a given <tt>role</tt> is allowed to execute a given <tt>operation</tt> on the namespace.
     *
     * @param namespaceName namespace name
     * @param role role name
     * @param operation namespace operation
     * @param authData authenticated data
     * @return a completable future represents check result
     */
    default CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                    String role,
                                                                    NamespaceOperation operation,
                                                                    AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(
            new IllegalStateException("NamespaceOperation [" + operation.name() + "] is not supported by "
                    + "the Authorization provider you are using."));
    }

    /**
     * Check if a given <tt>role</tt> is allowed to execute a given policy <tt>operation</tt> on the namespace.
     *
     * @param namespaceName namespace name
     * @param policy policy name
     * @param operation policy operation
     * @param role role name
     * @param authData authenticated data
     * @return a completable future represents check result
     */
    default CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                          PolicyName policy,
                                                                          PolicyOperation operation,
                                                                          String role,
                                                                          AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(
                new IllegalStateException("NamespacePolicyOperation  [" + policy.name() + "/" + operation.name() + "] "
                        + "is not supported by is not supported by the Authorization provider you are using."));
    }

    /**
     * Check if a given <tt>role</tt> is allowed to execute a given topic <tt>operation</tt> on the topic.
     *
     * @param topic topic name
     * @param role role name
     * @param operation topic operation
     * @param authData authenticated data
     * @return CompletableFuture<Boolean>
     */
    default CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic,
                                                                String role,
                                                                TopicOperation operation,
                                                                AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(
            new IllegalStateException("TopicOperation [" + operation.name() + "] is not supported by the Authorization"
                    + "provider you are using."));
    }

    /**
     * Check if a given <tt>role</tt> is allowed to execute a given topic <tt>operation</tt> on topic's <tt>policy</tt>.
     *
     * @param topic topic name
     * @param role role name
     * @param operation topic operation
     * @param authData authenticated data
     * @return CompletableFuture<Boolean>
     */
    default CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topic,
                                                                      String role,
                                                                      PolicyName policy,
                                                                      PolicyOperation operation,
                                                                      AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(
                new IllegalStateException("TopicPolicyOperation [" + policy.name() + "/" + operation.name() + "] "
                        + "is not supported by the Authorization provider you are using."));
    }

    /**
     * Remove authorization-action permissions on a topic.
     * @param topicName
     * @return CompletableFuture<Void>
     */
    default CompletableFuture<Void> removePermissionsAsync(TopicName topicName) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Get authorization-action permissions on a topic.
     * @param topicName
     * @return CompletableFuture<Map<String, Set<AuthAction>>>
     */
    default CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(TopicName topicName) {
        return FutureUtil.failedFuture(new IllegalStateException(
                String.format("getPermissionsAsync on topicName %s is not supported by the Authorization",
                        topicName)));
    }

    /**
     * Get authorization-action permissions on a topic.
     * @param namespaceName
     * @return CompletableFuture<Map<String, Set<String>>>
     */
    default CompletableFuture<Map<String, Set<String>>> getSubscriptionPermissionsAsync(NamespaceName namespaceName) {
        return FutureUtil.failedFuture(new IllegalStateException(
                String.format("getSubscriptionPermissionsAsync on namespace %s is not supported by the Authorization",
                        namespaceName)));
    }

    /**
     * Get authorization-action permissions on a namespace.
     * @param namespaceName
     * @return CompletableFuture<Map<String, Set<AuthAction>>>
     */
    default CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(NamespaceName namespaceName) {
        return FutureUtil.failedFuture(new IllegalStateException(
                String.format("getPermissionsAsync on namespaceName %s is not supported by the Authorization",
                        namespaceName)));
    }
}
