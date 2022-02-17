---
id: version-2.7.4-security-extending
title: Extending Authentication and Authorization in Pulsar
sidebar_label: Extending
original_id: security-extending
---

Pulsar provides a way to use custom authentication and authorization mechanisms.

## Authentication

Pulsar supports mutual TLS and Athenz authentication plugins. For how to use these authentication plugins, you can refer to the description in [Security](security-overview.md).

You can use a custom authentication mechanism by providing the implementation in the form of two plugins. One plugin is for the Client library and the other plugin is for the Pulsar Proxy and/or Pulsar Broker to validate the credentials.

### Client authentication plugin

For the client library, you need to implement `org.apache.pulsar.client.api.Authentication`. By entering the command below you can pass this class when you create a Pulsar client:

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .authentication(new MyAuthentication())
    .build();
```

You can use 2 interfaces to implement on the client side:
 * `Authentication` -> http://pulsar.apache.org/api/client/org/apache/pulsar/client/api/Authentication.html
 * `AuthenticationDataProvider` -> http://pulsar.apache.org/api/client/org/apache/pulsar/client/api/AuthenticationDataProvider.html


This in turn needs to provide the client credentials in the form of `org.apache.pulsar.client.api.AuthenticationDataProvider`. This leaves the chance to return different kinds of authentication token for different types of connection or by passing a certificate chain to use for TLS.


You can find examples for client authentication providers at:

 * Mutual TLS Auth -- https://github.com/apache/pulsar/tree/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/auth
 * Athenz -- https://github.com/apache/pulsar/tree/master/pulsar-client-auth-athenz/src/main/java/org/apache/pulsar/client/impl/auth

### Proxy/Broker authentication plugin

On the proxy/broker side, you need to configure the corresponding plugin to validate the credentials that the client sends. The Proxy and Broker can support multiple authentication providers at the same time.

In `conf/broker.conf` you can choose to specify a list of valid providers:

```properties
# Authentication provider name list, which is comma separated list of class names
authenticationProviders=
```
To implement `org.apache.pulsar.broker.authentication.AuthenticationProvider` on one single interface:

```java
/**
 * Provider of authentication mechanism
 */
public interface AuthenticationProvider extends Closeable {

    /**
     * Perform initialization for the authentication provider
     *
     * @param config
     *            broker config object
     * @throws IOException
     *             if the initialization fails
     */
    void initialize(ServiceConfiguration config) throws IOException;

    /**
     * @return the authentication method name supported by this provider
     */
    String getAuthMethodName();

    /**
     * Validate the authentication for the given credentials with the specified authentication data
     *
     * @param authData
     *            provider specific authentication data
     * @return the "role" string for the authenticated connection, if the authentication was successful
     * @throws AuthenticationException
     *             if the credentials are not valid
     */
    String authenticate(AuthenticationDataSource authData) throws AuthenticationException;

}
```

The following is the example for Broker authentication plugins:

 * Mutual TLS -- https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProviderTls.java
 * Athenz -- https://github.com/apache/pulsar/blob/master/pulsar-broker-auth-athenz/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProviderAthenz.java

## Authorization

Authorization is the operation that checks whether a particular "role" or "principal" has permission to perform a certain operation.

By default, you can use the embedded authorization provider provided by Pulsar. You can also configure a different authorization provider through a plugin.
Note that although the Authentication plugin is designed for use in both the Proxy and Broker,
the Authorization plugin is designed only for use on the Broker however the Proxy does perform some simple Authorization checks of Roles if authorization is enabled.

To provide a custom provider, you need to implement the `org.apache.pulsar.broker.authorization.AuthorizationProvider` interface, put this class in the Pulsar broker classpath and configure the class in `conf/broker.conf`:

 ```properties
 # Authorization provider fully qualified class-name
 authorizationProvider=org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider
 ```

```java
/**
 * Provider of authorization mechanism
 */
public interface AuthorizationProvider extends Closeable {

    /**
     * Perform initialization for the authorization provider
     *
     * @param conf
     *            broker config object
     * @param configCache
     *            pulsar zk configuration cache service
     * @throws IOException
     *             if the initialization fails
     */
    void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache) throws IOException;

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
     *
     * Grant authorization-action permission on a namespace to the given client
     *
     * @param namespace
     * @param actions
     * @param role
     * @param authDataJson
     *            additional authdata in json format
     * @return CompletableFuture
     * @completesWith <br/>
     *                IllegalArgumentException when namespace not found<br/>
     *                IllegalStateException when failed to grant permission
     */
    CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
            String authDataJson);

    /**
     * Grant authorization-action permission on a topic to the given client
     *
     * @param topicName
     * @param role
     * @param authDataJson
     *            additional authdata in json format
     * @return CompletableFuture
     * @completesWith <br/>
     *                IllegalArgumentException when namespace not found<br/>
     *                IllegalStateException when failed to grant permission
     */
    CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role,
            String authDataJson);

}

```
