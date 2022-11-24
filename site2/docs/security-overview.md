---
id: security-overview
title: Pulsar security overview
sidebar_label: "Overview"
---

As the central message bus for a business, Apache Pulsar is frequently used for storing mission-critical data. Therefore, enabling security features in Pulsar is crucial. This chapter describes the main security controls that Pulsar uses to help protect your data.

Pulsar security is based on the following core pillars.
- [Encryption](#encryption)
- [Authentication](#authentication)
- [Authorization](#authorization)

By default, Pulsar configures no encryption, authentication, or authorization. Any clients can communicate to Pulsar via plain text service URLs. So you must ensure that Pulsar accessing via these plain text service URLs is restricted to trusted clients only. In such cases, you can use network segmentation and/or authorization ACLs to restrict access to trusted IPs. If you use neither, the state of the cluster is wide open and anyone can access the cluster.

Apache Pulsar uses an [Authentication Provider](#authentication) or an [Authentication Provider Chain](security-extending.md#proxybroker-authentication-plugin) to establish the identity of a client and then assign a *role token* (a string like `admin` or `app1`) to that client. This role token can represent a single client or multiple clients and is then used for [Authorization](security-authorization.md) to determine what the client is authorized to do. You can use roles to control permission for clients to produce or consume from certain topics, administer the configuration for tenants, and so on.

## Encryption

Encryption ensures that if an attacker gets access to your data, the attacker cannot read the data without also having access to the encryption keys. Encryption provides an important mechanism for protecting your data in-transit to meet your security requirements for cryptographic algorithms and key management. 

**What's next?**

- To configure end-to-end encryption, see [End-to-end encryption](security-encryption.md) for more details.
- To configure transport layer encryption, see [TLS encryption](security-tls-transport.md) for more details.

## Authentication

Authentication is the process of verifying the identity of clients. In Pulsar, the authentication provider is responsible for properly identifying clients and associating them with role tokens. Note that if you only enable authentication, an authenticated role token can access all resources in the cluster. 

### How it works in Pulsar

Pulsar provides a pluggable authentication framework, and Pulsar brokers/proxies use this mechanism to authenticate clients.

The way how each client passes its authentication data to brokers varies depending on the protocols it uses. Brokers validate the authentication credentials when a connection is established and check whether the authentication data is expired.
- When using HTTP/HTTPS protocol for cluster management, each client passes the authentication data based on the HTTP/HTTPS request header, and brokers check the data upon request.
- When using [Pulsar protocol](developing-binary-protocol.md) for productions/consumptions, each client passes the authentication data by sending the `CommandConnect` command when connecting to brokers. Brokers cache the data and periodically check whether the data has expired and learn whether the client supports authentication refreshing. By default, the `authenticationRefreshCheckSeconds` is set to 60s.
  - If a client supports authentication refreshing and the credential is expired, brokers send the `CommandAuthChallenge` command to exchange the authentication data with the client. If the next check finds that the previous authentication exchange has not been returned, brokers disconnect the client.
  - If a client does not support authentication refreshing and the credential is expired, brokers disconnect the client.

### Authentication data limitations on the proxies

When you use proxies between clients and brokers, there are two authentication data:
* authentication data from proxies that brokers default to authenticate - known as **self-authentication**.
* authentication data from clients that proxies forward to brokers for authenticating - known as **original authentication**.

**Important:** If your authentication data contains an expiration time, or your authorization provider depends on the authentication data, you must:

1. Ensure your authentication data of proxies has no expiration time since brokers don't support refreshing this authentication data.
2. Set `forwardAuthorizationCredentials` to `true` in the `conf/proxy.conf` file.
3. Set `authenticateOriginalAuthData` to `true` in the `conf/broker.conf` file, which ensures that brokers recheck the client authentication.

**What's next?**

- To configure built-in authentication plugins, read:
  - [TLS authentication](security-tls-authentication.md)
  - [Athenz authentication](security-athenz.md)
  - [Kerberos authentication](security-kerberos.md)
  - [JSON Web Token (JWT) authentication](security-jwt.md)
  - [OAuth 2.0 authentication](security-oauth2.md)
  - [HTTP basic authentication](security-basic-auth.md)
- To customize an authentication plugin, read [extended authentication](security-extending).

:::note

Starting from 2.11.0, you can configure [Mutual TLS](security-tls-transport.md) with any one of the above authentication providers.

:::

## Authorization

[Authorization](security-authorization.md) is the process of giving permissions to clients and determining what clients can do.

The role tokens with the most permissions are the superusers who can create and delete tenants, along with full access to all tenant resources. When a superuser creates a tenant, that tenant is assigned an admin role token. A client with the admin role token can then create, modify and destroy namespaces, and grant and revoke permissions to other role tokens on those namespaces.
