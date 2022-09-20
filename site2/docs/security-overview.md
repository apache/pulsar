---
id: security-overview
title: Pulsar security overview
sidebar_label: "Overview"
---

As the central message bus for a business, Apache Pulsar is frequently used for storing mission-critical data. Therefore, enabling security features in Pulsar is crucial. This chapter describes the main security controls that Pulsar uses to help protect your data.

Pulsar security is based on the following core pillars.
* [Encryption](#encryption)
* [Authentication](#authentication)
* [Authorization](#authorization)

By default, Pulsar configures no encryption, authentication, or authorization. Any clients can communicate to Pulsar via plain text service URLs. So you must ensure that Pulsar accessing via these plain text service URLs is restricted to trusted clients only. In such cases, you can use network segmentation and/or authorization ACLs to restrict access to trusted IPs. If you use neither, the state of the cluster is wide open and anyone can access the cluster.

Apache Pulsar uses an [Authentication Provider](#authentication) or an [Authentication Provider Chain](security-extending.md/#proxybroker-authentication-plugin) to establish the identity of a client and then assign a *role token* (a string like `admin` or `app1`)to that client. This role token can represent a single client or multiple clients and is then used for [Authorization](security-authorization.md) to determine what the client is authorized to do. You can use roles to control permission for clients to produce or consume from certain topics, administer the configuration for tenants, and so on.

## Encryption

Encryption ensures that if an attacker gets access to your data, the attacker cannot read the data without also having access to the encryption keys. Encryption provides an important mechanism for protecting your data at-rest and in-transit to meet your security requirements for cryptographic algorithms and key management. 

**What's next?**
* To configure end-to-end encryption, see [End-to-end encryption](security-encryption.md) for more details.
* To configure transport layer encryption, see [TLS encryption](security-tls-transport.md) for more details.

## Authentication

Authentication is the process of verifying the identity of clients. In Pulsar, the authentication provider is responsible for properly identifying clients and associating the clients with role tokens. If you only enable authentication, an authenticated role token can access all resources in the cluster. 

Pulsar supports a pluggable authentication mechanism, and Pulsar clients use this mechanism to authenticate with brokers and proxies. 

Pulsar broker validates the authentication credentials when a connection is established. After the initial connection is authenticated, the "principal" token is stored for authorization though the connection is not re-authenticated. The broker periodically checks the expiration status of every `ServerCnx` object. By default, the `authenticationRefreshCheckSeconds` is set to 60s. When the authentication is expired, the broker re-authenticates the connection. If the re-authentication fails, the broker disconnects the client.

Pulsar broker supports learning whether a particular client supports authentication refreshing. If a client supports authentication refreshing and the credential is expired, the authentication provider calls the `refreshAuthentication` method to initiate the refreshing process. If a client does not support authentication refreshing and the credential is expired, the broker disconnects the client.

**What's next?**
Currently, Pulsar supports the following authentication providers:
- [TLS authentication](security-tls-authentication.md)
- [Athenz authentication](security-athenz.md)
- [Kerberos authentication](security-kerberos.md)
- [JSON Web Token (JWT) authentication](security-jwt.md)
- [OAuth 2.0 authentication](security-oauth2.md)
- [HTTP basic authentication](security-basic-auth.md)
You can also configure Pulsar to support multiple authentication providers.

:::note

Starting from 2.11.0, [TLS authentication](security-tls-authentication.md) includes [TLS encryption](security-tls-transport.md) by default. If you configure TLS authentication first, then TLS encryption automatically applies; if you configure TLS encryption first, you can select any one of the above authentication providers.

:::

## Authorization

[Authorization](security-authorization.md) is the process of giving permissions to clients and determining what clients can do.

The role tokens with the most permissions are the superusers who can create and delete tenants, along with full access to all tenant resources. When a superuser creates a tenant, that tenant is assigned an admin role token. A client with the admin role token can then create, modify and destroy namespaces, and grant and revoke permissions to other role tokens on those namespaces.
