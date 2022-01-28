---
id: security-token-admin
title: Token authentication admin
sidebar_label: "Token authentication admin"
---

## Token Authentication Overview

Pulsar supports authenticating clients using security tokens that are based on [JSON Web Tokens](https://jwt.io/introduction/) ([RFC-7519](https://tools.ietf.org/html/rfc7519)).

Tokens are used to identify a Pulsar client and associate with some "principal" (or "role") which
will be then granted permissions to do some actions (eg: publish or consume from a topic).

A user will typically be given a token string by an administrator (or some automated service).

The compact representation of a signed JWT is a string that looks like:

```

 eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY

```

Application will specify the token when creating the client instance. An alternative is to pass
a "token supplier", that is to say a function that returns the token when the client library
will need one.

> #### Always use TLS transport encryption
> Sending a token is equivalent to sending a password over the wire. It is strongly recommended to
> always use TLS encryption when talking to the Pulsar service. See
> [Transport Encryption using TLS](security-tls-transport)

## Secret vs Public/Private keys

JWT support two different kind of keys in order to generate and validate the tokens:

 * Symmetric :
    - there is a single ***Secret*** key that is used both to generate and validate
 * Asymmetric: there is a pair of keys.
    - ***Private*** key is used to generate tokens
    - ***Public*** key is used to validate tokens

### Secret key

When using a secret key, the administrator will create the key and he will
use it to generate the client tokens. This key will be also configured to
the brokers to allow them to validate the clients.

#### Creating a secret key

> Output file will be generated in the root of your pulsar installation directory. You can also provide absolute path for the output file.

```shell

$ bin/pulsar tokens create-secret-key --output my-secret.key

```

To generate base64 encoded private key

```shell

$ bin/pulsar tokens create-secret-key --output  /opt/my-secret.key --base64

```

### Public/Private keys

With public/private, we need to create a pair of keys. Pulsar supports all algorithms supported by the Java JWT library shown [here](https://github.com/jwtk/jjwt#signature-algorithms-keys)

#### Creating a key pair

> Output file will be generated in the root of your pulsar installation directory. You can also provide absolute path for the output file.

```shell

$ bin/pulsar tokens create-key-pair --output-private-key my-private.key --output-public-key my-public.key

```

 * `my-private.key` will be stored in a safe location and only used by administrator to generate
   new tokens.
 * `my-public.key` will be distributed to all Pulsar brokers. This file can be publicly shared without
   any security concern.

## Generating tokens

A token is the credential associated with a user. The association is done through the "principal",
or "role". In case of JWT tokens, this field it's typically referred to as **subject**, though
it's exactly the same concept.

The generated token is then required to have a **subject** field set.

```shell

$ bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
            --subject test-user

```

This will print the token string on stdout.

Similarly, one can create a token by passing the "private" key:

```shell

$ bin/pulsar tokens create --private-key file:///path/to/my-private.key \
            --subject test-user

```

Finally, a token can also be created with a pre-defined TTL. After that time,
the token will be automatically invalidated.

```shell

$ bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
            --subject test-user \
            --expiry-time 1y

```

## Authorization

The token itself doesn't have any permission associated. That will be determined by the
authorization engine. Once the token is created, one can grant permission for this token to do certain
actions. Eg. :

```shell

$ bin/pulsar-admin namespaces grant-permission my-tenant/my-namespace \
            --role test-user \
            --actions produce,consume

```

## Enabling Token Authentication ...

### ... on Brokers

To configure brokers to authenticate clients, put the following in `broker.conf`:

```properties

# Configuration to enable authentication and authorization
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# If using secret key (Note: key files must be DER-encoded)
tokenSecretKey=file:///path/to/secret.key
# The key can also be passed inline:
# tokenSecretKey=data:;base64,FLFyW0oLJ2Fi22KKCm21J18mbAdztfSHN/lAT5ucEKU=

# If using public/private (Note: key files must be DER-encoded)
# tokenPublicKey=file:///path/to/public.key

```

### ... on Proxies

To configure proxies to authenticate clients, put the following in `proxy.conf`:

The proxy will have its own token used when talking to brokers. The role token for this
key pair should be configured in the ``proxyRoles`` of the brokers. See the [authorization guide](security-authorization) for more details.

```properties

# For clients connecting to the proxy
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken
tokenSecretKey=file:///path/to/secret.key

# For the proxy to connect to brokers
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters={"token":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.9OHgE9ZUDeBTZs7nSMEFIuGNEX18FLR3qvy8mqxSxXw"}
# Or, alternatively, read token from file
# brokerClientAuthenticationParameters=file:///path/to/proxy-token.txt

```

