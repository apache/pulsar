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
package org.apache.pulsar.broker.authentication.oidc;

import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * These are the modes available for configuring how the Open ID Connect Authentication Provider should handle a JWT
 * that has an issuer that is not explicitly in the allowed issuers set configured by
 * {@link AuthenticationProviderOpenID#ALLOWED_TOKEN_ISSUERS}. The current implementations rely on using the Kubernetes
 * Api Server's Open ID Connect features to discover an additional issuer or additional public keys to trust. See the
 * Kubernetes documentation for more information on how Service Accounts can integrate with Open ID Connect.
 * https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#service-account-issuer-discovery
 */
@InterfaceStability.Evolving
public enum FallbackDiscoveryMode {
    /**
     * There will be no discovery of additional trusted issuers or public keys. This setting requires that operators
     * explicitly allow all issuers that will be trusted. For the Kubernetes Service Account Token Projections to work,
     * the operator must explicitly trust the issuer on the token's "iss" claim. This is the default setting because it
     * is the only mode that explicitly follows the OIDC spec for verification of discovered provider configuration.
     */
    DISABLED,

    /**
     * The Kubernetes Api Server will be used to discover an additional trusted issuer by getting the issuer at the
     * Api Server's /.well-known/openid-configuration endpoint, verifying that issuer matches the "iss" claim on the
     * supplied token, then treating that issuer as a trusted issuer by discovering the jwks_uri via that issuer's
     * /.well-known/openid-configuration endpoint. This mode can be helpful in EKS environments where the Api Server's
     * public keys served at the /openid/v1/jwks endpoint are not the same as the public keys served at the issuer's
     * jwks_uri. It fails to be OIDC compliant because the URL used to discover the provider configuration is not the
     * same as the issuer claim on the token.
     */
    KUBERNETES_DISCOVER_TRUSTED_ISSUER,

    /**
     * The Kubernetes Api Server will be used to discover an additional set of valid public keys by getting the issuer
     * at the Api Server's /.well-known/openid-configuration endpoint, verifying that issuer matches the "iss" claim on
     * the supplied token, then calling the Api Server endpoint to get the public keys using a kubernetes client. This
     * mode is currently useful getting the public keys from the Api Server because the Api Server requires custom TLS
     * and authentication, and the kubernetes client automatically handles those. It fails to be OIDC compliant because
     * the URL used to discover the provider configuration is not the same as the issuer claim on the token.
     */
    KUBERNETES_DISCOVER_PUBLIC_KEYS,
}
