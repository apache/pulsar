---
id: version-2.8.0-security-jwt
title: Client authentication using tokens based on JSON Web Tokens
sidebar_label: Authentication using JWT
original_id: security-jwt
---

## Token authentication overview

Pulsar supports authenticating clients using security tokens that are based on
[JSON Web Tokens](https://jwt.io/introduction/) ([RFC-7519](https://tools.ietf.org/html/rfc7519)).

You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that
is permitted to do some actions (eg: publish to a topic or consume from a topic).

A user typically gets a token string from the administrator (or some automated service).

The compact representation of a signed JWT is a string that looks like as the following:

```
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

Application specifies the token when you create the client instance. An alternative is to pass a "token supplier" (a function that returns the token when the client library needs one).

> #### Always use TLS transport encryption
> Sending a token is equivalent to sending a password over the wire. You had better use TLS encryption all the time when you connect to the Pulsar service. See
> [Transport Encryption using TLS](security-tls-transport.md) for more details.

### CLI Tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-pulsar-admin.md), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You need to add the following parameters to that file to use the token authentication with CLI tools of Pulsar:

```properties
webServiceUrl=http://broker.example.com:8080/
brokerServiceUrl=pulsar://broker.example.com:6650/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
authParams=token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

The token string can also be read from a file, for example:
```
authParams=file:///path/to/token/file
```

### Pulsar client

You can use tokens to authenticate the following Pulsar clients.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY")）
    .build();
```

Similarly, you can also pass a `Supplier`:

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactory.token(() -> {
            // Read token from custom source
            return readToken();
        }))
    .build();
```

<!--Python-->
```python
from pulsar import Client, AuthenticationToken

client = Client('pulsar://broker.example.com:6650/'
                authentication=AuthenticationToken('eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY'))
```

Alternatively, you can also pass a `Supplier`:

```python

def read_token():
    with open('/path/to/token.txt') as tf:
        return tf.read().strip()

client = Client('pulsar://broker.example.com:6650/'
                authentication=AuthenticationToken(read_token))
```

<!--Go-->
```go
client, err := NewClient(ClientOptions{
	URL:            "pulsar://localhost:6650",
	Authentication: NewAuthenticationToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY"),
})
```
Similarly, you can also pass a `Supplier`:

```go
client, err := NewClient(ClientOptions{
	URL:            "pulsar://localhost:6650",
	Authentication: NewAuthenticationTokenSupplier(func () string {
        // Read token from custom source
		return readToken()
	}),
})
```

<!--C++-->
```c++
#include <pulsar/Client.h>

pulsar::ClientConfiguration config;
config.setAuth(pulsar::AuthToken::createWithToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY"));

pulsar::Client client("pulsar://broker.example.com:6650/", config);
```

<!--C#-->
```c#
var client = PulsarClient.Builder()
                         .AuthenticateUsingToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY")
                         .Build();
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Enable token authentication 

On how to enable token authentication on a Pulsar cluster, you can refer to the guide below.

JWT supports two different kinds of keys in order to generate and validate the tokens:

 * Symmetric :
    - You can use a single ***Secret*** key to generate and validate tokens.
 * Asymmetric: A pair of keys consists of the Private key and the Public key.
    - You can use ***Private*** key to generate tokens.
    - You can use ***Public*** key to validate tokens.

### Create a secret key

When you use a secret key, the administrator creates the key and uses the key to generate the client tokens. You can also configure this key to brokers in order to validate the clients.

Output file is generated in the root of your Pulsar installation directory. You can also provide absolute path for the output file using the command below.

```shell
$ bin/pulsar tokens create-secret-key --output my-secret.key
```

Enter this command to generate base64 encoded private key.

```shell
$ bin/pulsar tokens create-secret-key --output  /opt/my-secret.key --base64
```

### Create a key pair

With Public and Private keys, you need to create a pair of keys. Pulsar supports all algorithms that the Java JWT library (shown [here](https://github.com/jwtk/jjwt#signature-algorithms-keys)) supports.

Output file is generated in the root of your Pulsar installation directory. You can also provide absolute path for the output file using the command below.
```shell
$ bin/pulsar tokens create-key-pair --output-private-key my-private.key --output-public-key my-public.key
```

 * Store `my-private.key` in a safe location and only administrator can use `my-private.key` to generate new tokens.
 * `my-public.key` is distributed to all Pulsar brokers. You can publicly share this file without any security concern.

### Generate tokens

A token is the credential associated with a user. The association is done through the "principal" or "role". In the case of JWT tokens, this field is typically referred as **subject**, though they are exactly the same concept.

Then, you need to use this command to require the generated token to have a **subject** field set.

```shell
$ bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
            --subject test-user
```

This command prints the token string on stdout.

Similarly, you can create a token by passing the "private" key using the command below:

```shell
$ bin/pulsar tokens create --private-key file:///path/to/my-private.key \
            --subject test-user
```

Finally, you can enter the following command to create a token with a pre-defined TTL. And then the token is automatically invalidated.

```shell
$ bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
            --subject test-user \
            --expiry-time 1y
```

### Authorization

The token itself does not have any permission associated. The authorization engine determines whether the token should have permissions or not. Once you have created the token, you can grant permission for this token to do certain actions. The following is an example.

```shell
$ bin/pulsar-admin namespaces grant-permission my-tenant/my-namespace \
            --role test-user \
            --actions produce,consume
```

### Enable token authentication on Brokers

To configure brokers to authenticate clients, add the following parameters to `broker.conf`:

```properties
# Configuration to enable authentication and authorization
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# Authentication settings of the broker itself. Used when the broker connects to other brokers, either in same or other clusters
brokerClientTlsEnabled=true
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters={"token":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.9OHgE9ZUDeBTZs7nSMEFIuGNEX18FLR3qvy8mqxSxXw"}
# Or, alternatively, read token from file
# brokerClientAuthenticationParameters={"file":"///path/to/proxy-token.txt"}
brokerClientTrustCertsFilePath=/path/my-ca/certs/ca.cert.pem

# If this flag is set then the broker authenticates the original Auth data
# else it just accepts the originalPrincipal and authorizes it (if required).
authenticateOriginalAuthData=true

# If using secret key (Note: key files must be DER-encoded)
tokenSecretKey=file:///path/to/secret.key
# The key can also be passed inline:
# tokenSecretKey=data:;base64,FLFyW0oLJ2Fi22KKCm21J18mbAdztfSHN/lAT5ucEKU=

# If using public/private (Note: key files must be DER-encoded)
# tokenPublicKey=file:///path/to/public.key
```

### Enable token authentication on Proxies

To configure proxies to authenticate clients, add the following parameters to `proxy.conf`:

The proxy uses its own token when connecting to brokers. You need to configure the role token for this key pair in the `proxyRoles` of the brokers. For more details, see the [authorization guide](security-authorization.md).

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
# brokerClientAuthenticationParameters={"file":"///path/to/proxy-token.txt"}

# Whether client authorization credentials are forwarded to the broker for re-authorization.
# Authentication must be enabled via authenticationEnabled=true for this to take effect.
forwardAuthorizationCredentials=true
```
