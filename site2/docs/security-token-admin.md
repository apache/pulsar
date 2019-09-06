---
id: security-token-admin
title: Token authentication admin
sidebar_label: Token authentication admin
---

## Token Authentication Overview

Pulsar supports authenticating clients using security tokens that are based on [JSON Web Tokens](https://jwt.io/introduction/) ([RFC-7519](https://tools.ietf.org/html/rfc7519)).

You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that is permitted to do some actions (for example, publish to a topic or consume from a topic).

A user typically gets a token string from the administrator (or some automated service).

The compact representation of a signed JWT is a string that looks like as the follwing:

```
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

The token is specified by application when you create a client instance. An alternative is to pass a "token supplier" (a function that returns the token when the client library needs one).


> #### Always use TLS transport encryption
> Sending a token is equivalent to sending a password over the wire. You had better
> use TLS encryption all the time when you connect to the Pulsar service. See
> [Transport Encryption using TLS](security-tls-transport.md) for more details.

## Secret vs Public and Private keys

JWT support two different kinds of keys in order to generate and validate the tokens:

 * Symmetric :
    - You can use a single ***Secret*** key to generate and validate tokens.
 * Asymmetric: A pair of keys consist of the Private key and the Public key.
    - You can use ***Private*** key to generate tokens.
    - You can use ***Public*** key to validate tokens.

### Secret key

When you use a secret key, the administrator creates the key and uses the key to generate the client tokens. You can also configure this key to the brokers in order to allow the brokers validating the clients.

#### Create a secret key

> Output file is generated in the root of your pulsar installation directory. You can also enter the command below to provide absolute path for the output file.
```shell
$ bin/pulsar tokens create-secret-key --output my-secret.key
```
Enter this command to generate base64 encoded private key.
```shell
$ bin/pulsar tokens create-secret-key --output  /opt/my-secret.key --base64
```

### Public keys and Private keys

With public keys or private keys, we need to create a pair of keys. Pulsar supports all algorithms that the Java JWT library (shown [here](https://github.com/jwtk/jjwt#signature-algorithms-keys)) supported.

#### Create a key pair

> Output file is generated in the root of your pulsar installation directory. You can also enter the command below to provide absolute path for the output file.
```shell
$ bin/pulsar tokens create-key-pair --output-private-key my-private.key --output-public-key my-public.key
```

 * Store `my-private.key` in a safe location and only administrator can use `my-private.key` to generate new tokens.
 * `my-public.key` is distributed to all Pulsar brokers. You can publicly share this file without any security concern.

## Generate tokens

A token is the credential associated with a user. The association is done through the "principal" or "role". In the case of JWT tokens, we typically refer to this field as **subject**, though they are exactly the same concept.

Then, you need to enter this command to require the generated token to have a **subject** field set .

```shell
$ bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
            --subject test-user
```

This command prints the token string on stdout.

Similarly, you can enter the command below to create a token by passing the "private" key:

```shell
$ bin/pulsar tokens create --private-key file:///path/to/my-private.key \
            --subject test-user
```

Finally, you can also enter the command below to create a token with a pre-defined TTL. After that, the token is automatically invalidated.

```shell
$ bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
            --subject test-user \
            --expiry-time 1y
```

## Authorization

The token does not associate any permission. The authorization engine determines whether the token has permissions or not. Once you have created the token, you can grant permission for this token to do certain actions. For example:

```shell
$ bin/pulsar-admin namespaces grant-permission my-tenant/my-namespace \
            --role test-user \
            --actions produce,consume
```

## Enable token authentication on Brokers

To configure brokers to authenticate clients, add the following parameters to `broker.conf`:

```properties
# Configuration to enable authentication and authorization
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# If using secret key
tokenSecretKey=file:///path/to/secret.key
# The key can also be passed inline:
# tokenSecretKey=data:base64,FLFyW0oLJ2Fi22KKCm21J18mbAdztfSHN/lAT5ucEKU=

# If using public/private
# tokenPublicKey=file:///path/to/public.key
```

## Enable token authentication on Proxies

To configure proxies to authenticate clients, add the following parameters to `proxy.conf`:

The proxy has its own token used when the proxy talks to brokers. You need to configure the role token for this key pair in the ``proxyRoles`` of the brokers. See the [authorization guide](security-authorization.md) for more details.

```properties
# For clients connecting to the proxy
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken
tokenSecretKey=file:///path/to/secret.key

# For the proxy to connect to brokers
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters=token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.9OHgE9ZUDeBTZs7nSMEFIuGNEX18FLR3qvy8mqxSxXw
# Or, alternatively, read token from file
# brokerClientAuthenticationParameters=file:///path/to/proxy-token.txt
```
