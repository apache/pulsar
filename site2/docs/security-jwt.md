---
id: security-jwt
title: Authentication using tokens based on JSON Web Tokens
sidebar_label: "Authentication using JWT"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

Pulsar supports authenticating clients using security tokens based on [JSON Web Tokens](https://jwt.io/introduction/) ([RFC-7519](https://tools.ietf.org/html/rfc7519)), including all the algorithms that the [Java JWT library](https://github.com/jwtk/jjwt#signature-algorithms-keys) supports.

A token is a credential associated with a user. The association is done through a "principal" or "role". In the case of JWT tokens, it typically refers to a **subject**. You can use a token to identify a Pulsar client and associate it with a **subject** that is permitted to do specific actions, such as publish messages to a topic or consume messages from a topic. An alternative is to pass a "token supplier" (a function that returns the token when the client library needs one).

The application specifies the token when you create the client instance. The user typically gets the token string from the administrator. The compact representation of a signed JWT is a string that looks like the following:

```
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

:::note

Always use [TLS encryption](security-tls-transport.md) when connecting to the Pulsar service, because sending a token is equivalent to sending a password over the wire.

:::

## Create client certificates

JWT authentication supports two different kinds of keys to generate and validate the tokens:

- Symmetric: A single ***secret*** key.
- Asymmetric: A key pair, including:
  - a ***private*** key to generate tokens.
  - a ***public*** key to validate tokens.

### Create a secret key

The administrators create the secret key and use it to generate the client tokens. You can also configure this key for brokers to validate the clients.

The output file is generated in the root of your Pulsar installation directory. You can also provide an absolute path for the output file using the command below.

```shell
bin/pulsar tokens create-secret-key --output my-secret.key
```

To generate a base64-encoded private key, enter the following command.

```shell
bin/pulsar tokens create-secret-key --output  /opt/my-secret.key --base64
```

### Create a key pair

To use asymmetric key encryption, you need to create a pair of keys. The output file is generated in the root of your Pulsar installation directory. You can also provide an absolute path for the output file using the command below.

```shell
bin/pulsar tokens create-key-pair --output-private-key my-private.key --output-public-key my-public.key
```

 * Store `my-private.key` in a safe location and only the administrators can use this private key to generate new tokens.
 * The public key file `my-public.key` is distributed to all Pulsar brokers. You can publicly share it without any security concerns.

### Generate tokens

1. Use this command to require the generated token to have a **subject** fieldset. This command prints the token string on `stdout`.

   ```shell
   bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
               --subject test-user
   ```

2. Create a token by passing the "private" key using the command below:

   ```shell
   bin/pulsar tokens create --private-key file:///path/to/my-private.key \
               --subject test-user
   ```

3. Create a token with a pre-defined TTL. Then the token is automatically invalidated.

   ```shell
   bin/pulsar tokens create --secret-key file:///path/to/my-secret.key \
               --subject test-user \
               --expiry-time 1y
   ```

:::tip

The token itself does not have any permission associated. You need to [enable authorization and assign superusers](security-authorization.md#enable-authorization-and-assign-superusers), and use the `bin/pulsar-admin namespaces grant-permission` command to grant permissions to the token.

:::

## Enable JWT authentication on brokers/proxies

To configure brokers/proxies to authenticate clients using JWT, add the following parameters to the `conf/broker.conf` and the `conf/proxy.conf` file. If you use a standalone Pulsar, you need to add these parameters to the `conf/standalone.conf` file:

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# Authentication settings of the broker itself. Used when the broker connects to other brokers, or when the proxy connects to brokers, either in same or other clusters
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters={"token":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.9OHgE9ZUDeBTZs7nSMEFIuGNEX18FLR3qvy8mqxSxXw"}
# Either configure the token string or specify to read it from a file. The following three available formats are all valid:
# brokerClientAuthenticationParameters={"token":"your-token-string"}
# brokerClientAuthenticationParameters=token:your-token-string
# brokerClientAuthenticationParameters=file:///path/to/token

# If using secret key (Note: key files must be DER-encoded)
tokenSecretKey=file:///path/to/secret.key
# The key can also be passed inline:
# tokenSecretKey=data:;base64,FLFyW0oLJ2Fi22KKCm21J18mbAdztfSHN/lAT5ucEKU=

# If using public/private (Note: key files must be DER-encoded)
# tokenPublicKey=file:///path/to/public.key
```

## Configure JWT authentication in CLI Tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](/tools/pulsar-admin/), [`pulsar-perf`](reference-cli-tools.md), and [`pulsar-client`](reference-cli-tools.md) use the `conf/client.conf` config file in a Pulsar installation.

You need to add the following parameters to the `conf/client.conf` config file to use the JWT authentication with CLI tools of Pulsar:

```properties
webServiceUrl=https://broker.example.com:8443/
brokerServiceUrl=pulsar://broker.example.com:6650/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
authParams=token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

The token string can also be read from a file, for example:

```properties
authParams=file:///path/to/token/file
```

## Configure JWT authentication in Pulsar clients

You can use tokens to authenticate the following Pulsar clients.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"},{"label":"C++","value":"C++"},{"label":"C#","value":"C#"}]}>
<TabItem value="Java">

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

</TabItem>
<TabItem value="Python">

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

</TabItem>
<TabItem value="Go">

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
	URL:            "pulsar://localhost:6650",
	Authentication: NewAuthenticationToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY"),
})
```

Similarly, you can also pass a `Supplier`:

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
	URL:            "pulsar://localhost:6650",
	Authentication: pulsar.NewAuthenticationTokenSupplier(func () string {
        // Read token from custom source
		return readToken()
	}),
})
```

</TabItem>
<TabItem value="C++">

```cpp
#include <pulsar/Client.h>

pulsar::ClientConfiguration config;
config.setAuth(pulsar::AuthToken::createWithToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY"));

pulsar::Client client("pulsar://broker.example.com:6650/", config);
```

</TabItem>
<TabItem value="C#">

```csharp
var client = PulsarClient.Builder()
                         .AuthenticateUsingToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY")
                         .Build();
```

</TabItem>

</Tabs>
````