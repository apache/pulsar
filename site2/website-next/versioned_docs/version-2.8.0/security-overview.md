---
id: security-overview
title: Pulsar security overview
sidebar_label: Overview
original_id: security-overview
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


As the central message bus for a business, Apache Pulsar is frequently used for storing mission-critical data. Therefore, enabling security features in Pulsar is crucial.

By default, Pulsar configures no encryption, authentication, or authorization. Any client can communicate to Apache Pulsar via plain text service URLs. So we must ensure that Pulsar accessing via these plain text service URLs is restricted to trusted clients only. In such cases, you can use Network segmentation and/or authorization ACLs to restrict access to trusted IPs. If you use neither, the state of cluster is wide open and anyone can access the cluster.

Pulsar supports a pluggable authentication mechanism. And Pulsar clients use this mechanism to authenticate with brokers and proxies. You can also configure Pulsar to support multiple authentication sources.

The Pulsar broker validates the authentication credentials when a connection is established. After the initial connection is authenticated, the "principal" token is stored for authorization though the connection is not re-authenticated. The broker periodically checks the expiration status of every `ServerCnx` object. You can set the `authenticationRefreshCheckSeconds` on the broker to control the frequency to check the expiration status. By default, the `authenticationRefreshCheckSeconds` is set to 60s. When the authentication is expired, the broker forces to re-authenticate the connection. If the re-authentication fails, the broker disconnects the client.

The broker supports learning whether a particular client supports authentication refreshing. If a client supports authentication refreshing and the credential is expired, the authentication provider calls the `refreshAuthentication` method to initiate the refreshing process. If a client does not support authentication refreshing and the credential is expired, the broker disconnects the client.

You had better secure the service components in your Apache Pulsar deployment.

## Role tokens

In Pulsar, a *role* is a string, like `admin` or `app1`, which can represent a single client or multiple clients. You can use roles to control permission for clients to produce or consume from certain topics, administer the configuration for tenants, and so on.

Apache Pulsar uses a [Authentication Provider](#authentication-providers) to establish the identity of a client and then assign a *role token* to that client. This role token is then used for [Authorization and ACLs](security-authorization.md) to determine what the client is authorized to do.

## Authentication providers

Currently Pulsar supports the following authentication providers:

- [TLS Authentication](security-tls-authentication.md)
- [Athenz](security-athenz.md)
- [Kerberos](security-kerberos.md)
- [JSON Web Token Authentication](security-jwt.md)


