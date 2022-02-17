---
id: version-2.4.1-security-token-client
title: Client Authentication using tokens
sidebar_label: Client Authentication using tokens
original_id: security-token-client
---

## Token Authentication Overview

Pulsar supports authenticating clients using security tokens that are based on
[JSON Web Tokens](https://jwt.io/introduction/) ([RFC-7519](https://tools.ietf.org/html/rfc7519)).

You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that
is permitted to do some actions (for example, publish messages to a topic or consume messages from a topic).

The administrator (or some automated service) typically gives a user a token string.

The compact representation of a signed JWT is a string that looks like as the following:

```
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

Application specifies the token when you are creating the client instance. An alternative is to pass a "token supplier" (a function that returns the token when the client library needs one).

See [Token authentication admin](security-token-admin.md) for a reference on how to enable token
authentication on a Pulsar cluster.

### CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-pulsar-admin.md), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You need to add the following parameters to that file to use the token authentication with CLI tools of Pulsar:

```properties
webServiceUrl=http://broker.example.com:8080/
brokerServiceUrl=pulsar://broker.example.com:6650/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
authParams=token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

The token string can also be read from a file, eg:

```
authParams=file:///path/to/token/file
```

### Java client

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY")
    .build();
```

Similarly, one can also pass a `Supplier`:

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactory.token(() -> {
            // Read token from custom source
            return readToken();
        })
    .build();
```

### Python client

```python
from pulsar import Client, AuthenticationToken

client = Client('pulsar://broker.example.com:6650/'
                authentication=AuthenticationToken('eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY'))
```

Alternatively, with a supplier:

```python

def read_token():
    with open('/path/to/token.txt') as tf:
        return tf.read().strip()

client = Client('pulsar://broker.example.com:6650/'
                authentication=AuthenticationToken(read_token))
```

### Go client


```go
client, err := NewClient(ClientOptions{
	URL:            "pulsar://localhost:6650",
	Authentication: NewAuthenticationToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY"),
})
```

Alternatively, with a supplier:

```go
client, err := NewClient(ClientOptions{
	URL:            "pulsar://localhost:6650",
	Authentication: NewAuthenticationTokenSupplier(func () string {
        // Read token from custom source
		return readToken()
	}),
})
```

### C++ client

```c++
#include <pulsar/Client.h>

pulsar::ClientConfiguration config;
config.setAuth(pulsar::AuthToken::createWithToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY"));

pulsar::Client client("pulsar://broker.example.com:6650/", config);
```
