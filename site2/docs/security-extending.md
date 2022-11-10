---
id: security-extending
title: Extend Authentication and Authorization in Pulsar
sidebar_label: "Extend Authentication and Authorization"
---

Pulsar provides a way to use custom authentication and authorization mechanisms.

## Authentication

You can use a custom authentication mechanism by providing the implementation in the form of two plugins.
* Client authentication plugin `org.apache.pulsar.client.api.AuthenticationDataProvider` provides the authentication data for broker/proxy.
* Broker/Proxy authentication plugin `org.apache.pulsar.broker.authentication.AuthenticationProvider` authenticates the authentication data from clients.

### Client authentication plugin

For the client library, you need to implement `org.apache.pulsar.client.api.Authentication`. By entering the command below, you can pass this class when you create a Pulsar client.

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .authentication(new MyAuthentication())
    .build();
```

You can implement 2 interfaces on the client side:
 * [`Authentication`](/api/client/org/apache/pulsar/client/api/Authentication.html)
 * [`AuthenticationDataProvider`](/api/client/org/apache/pulsar/client/api/AuthenticationDataProvider.html)

This in turn requires you to provide the client credentials in the form of `org.apache.pulsar.client.api.AuthenticationDataProvider` and also leaves the chance to return different kinds of authentication tokens for different types of connections or by passing a certificate chain to use for TLS.

You can find the following examples for different client authentication plugins:
 * [Mutual TLS](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/auth/AuthenticationTls.java)
 * [Athenz](https://github.com/apache/pulsar/blob/master/pulsar-client-auth-athenz/src/main/java/org/apache/pulsar/client/impl/auth/AuthenticationAthenz.java)
 * [Kerberos](https://github.com/apache/pulsar/blob/master/pulsar-client-auth-sasl/src/main/java/org/apache/pulsar/client/impl/auth/AuthenticationSasl.java)
 * [JSON Web Token (JWT)](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/auth/AuthenticationToken.java)
 * [OAuth 2.0](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/auth/oauth2/AuthenticationOAuth2.java)
 * [Basic auth](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/auth/AuthenticationBasic.java)

### Broker/Proxy authentication plugin

On the broker/proxy side, you need to configure the corresponding plugin to validate the credentials that the client sends. The proxy and broker can support multiple authentication providers at the same time.

In `conf/broker.conf`, you can choose to specify a list of valid providers:

```properties
# Authentication provider name list, which is comma separated list of class names
authenticationProviders=
```

:::tip

Pulsar supports an authentication provider chain that contains multiple authentication providers with the same authentication method name. 

For example, your Pulsar cluster uses JSON Web Token (JWT) authentication (with an authentication method named `token`) and you want to upgrade it to use OAuth2.0 authentication with the same authentication name. In this case, you can implement your own authentication provider `AuthenticationProviderOAuth2` and configure `authenticationProviders` as follows.

```properties
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken,org.apache.pulsar.broker.authentication.AuthenticationProviderOAuth2
```

As a result, brokers look up the authentication providers with the `token` authentication method (JWT and OAuth2.0 authentication) when receiving requests to use the `token` authentication method. If a client cannot be authenticated via JWT authentication, OAuth2.0 authentication is used.

:::

For the implementation of the `org.apache.pulsar.broker.authentication.AuthenticationProvider` interface, refer to [code](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProvider.java).

You can find the following examples for different broker authentication plugins:

 * [Mutual TLS](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProviderTls.java)
 * [Athenz](https://github.com/apache/pulsar/blob/master/pulsar-broker-auth-athenz/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProviderAthenz.java)
 * [Kerberos](https://github.com/apache/pulsar/blob/master/pulsar-broker-auth-sasl/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProviderSasl.java)
 * [JSON Web Token (JWT)](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProviderToken.java)
 * [Basic auth](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProviderToken.java)

## Authorization

Authorization is the operation that checks whether a particular "role" or "principal" has permission to perform a certain operation.

By default, you can use the embedded authorization provider provided by Pulsar. You can also configure a different authorization provider through a plugin. Note that although the Authentication plugin is designed for use in both the proxy and broker, the Authorization plugin is designed only for use on the broker.

### Broker authorization plugin

To provide a custom authorization provider, you need to implement the `org.apache.pulsar.broker.authorization.AuthorizationProvider` interface, put this class in the Pulsar broker classpath and configure the class in `conf/broker.conf`:

 ```properties
 # Authorization provider fully qualified class-name
 authorizationProvider=org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider
 ```

For the implementation of the `org.apache.pulsar.broker.authorization.AuthorizationProvider` interface, refer to [code](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authorization/AuthorizationProvider.java).