---
id: security-oauth2
title: Client authentication using OAuth 2.0 access tokens
sidebar_label: "Authentication using OAuth 2.0 access tokens"
---

Pulsar supports authenticating clients using OAuth 2.0 access tokens. You can use OAuth 2.0 access tokens to identify a Pulsar client and associate the Pulsar client with some "principal" (or "role"), which is permitted to do some actions, such as publishing messages to a topic or consume messages from a topic.

This module is used to support the Pulsar client authentication plugin for OAuth 2.0. After communicating with the Oauth 2.0 server, the Pulsar client gets an `access token` from the Oauth 2.0 server, and passes this `access token` to the Pulsar broker to do the authentication. The broker can use the `org.apache.pulsar.broker.authentication.AuthenticationProviderToken`. Or, you can add your own `AuthenticationProvider` to make it with this module.

## Authentication provider configuration

This library allows you to authenticate the Pulsar client by using an access token that is obtained from an OAuth 2.0 authorization service, which acts as a _token issuer_.

### Authentication types

The authentication type determines how to obtain an access token through an OAuth 2.0 authorization flow.

#### Note
> Currently, the Pulsar Java client only supports the `client_credentials` authentication type .

#### Client credentials

The following table lists parameters supported for the `client credentials` authentication type.

| Parameter | Description | Example | Required or not |
| --- | --- | --- | --- |
| `type` | Oauth 2.0 authentication type. |  `client_credentials` (default) | Optional |
| `issuerUrl` | URL of the authentication provider which allows the Pulsar client to obtain an access token | `https://accounts.google.com` | Required |
| `privateKey` | URL to a JSON credentials file  | Support the following pattern formats: <br /> <li> `file:///path/to/file` </li><li>`file:/path/to/file` </li><li> `data:application/json;base64,<base64-encoded value>` </li>| Required |
| `audience`  | An OAuth 2.0 "resource server" identifier for the Pulsar cluster | `https://broker.example.com` | Optional |
| `scope` |  Scope of an access request. <br />For more more information, see [access token scope](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3). | api://pulsar-cluster-1/.default | Optional |

The credentials file contains service account credentials used with the client authentication type. The following shows an example of a credentials file `credentials_file.json`.

```json

{
  "type": "client_credentials",
  "client_id": "d9ZyX97q1ef8Cr81WHVC4hFQ64vSlDK3",
  "client_secret": "on1uJ...k6F6R",
  "client_email": "1234567890-abcdefghijklmnopqrstuvwxyz@developer.gserviceaccount.com",
  "issuer_url": "https://accounts.google.com"
}

```

In the above example, the authentication type is set to `client_credentials` by default. And the fields "client_id" and "client_secret" are required.

### Typical original OAuth2 request mapping

The following shows a typical original OAuth2 request, which is used to obtain the access token from the OAuth2 server.

```bash

curl --request POST \
  --url https://dev-kt-aa9ne.us.auth0.com \
  --header 'content-type: application/json' \
  --data '{
  "client_id":"Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
  "client_secret":"rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb",
  "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/",
  "grant_type":"client_credentials"}'

```

In the above example, the mapping relationship is shown as below.

- The `issuerUrl` parameter in this plugin is mapped to `--url https://dev-kt-aa9ne.us.auth0.com`.
- The `privateKey` file parameter in this plugin should at least contains the `client_id` and `client_secret` fields.
- The `audience` parameter in this plugin is mapped to  `"audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"`. This field is only used by some identity providers.

## Client Configuration

You can use the OAuth2 authentication provider with the following Pulsar clients.

### Java

You can use the factory method to configure authentication for Pulsar Java client.

```java

URL issuerUrl = new URL("https://dev-kt-aa9ne.us.auth0.com");
URL credentialsUrl = new URL("file:///path/to/KeyFile.json");
String audience = "https://dev-kt-aa9ne.us.auth0.com/api/v2/";

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactoryOAuth2.clientCredentials(issuerUrl, credentialsUrl, audience))
    .build();

```

In addition, you can also use the encoded parameters to configure authentication for Pulsar Java client.

```java

Authentication auth = AuthenticationFactory
    .create(AuthenticationOAuth2.class.getName(), "{"type":"client_credentials","privateKey":"./key/path/..","issuerUrl":"...","audience":"..."}");
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(auth)
    .build();

```

### C++ client

The C++ client is similar to the Java client. You need to provide parameters of `issuerUrl`, `private_key` (the credentials file path), and `audience`.

```c++

#include <pulsar/Client.h>

pulsar::ClientConfiguration config;
std::string params = R"({
    "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
    "private_key": "../../pulsar-broker/src/test/resources/authentication/token/cpp_credentials_file.json",
    "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";
    
config.setAuth(pulsar::AuthOauth2::create(params));

pulsar::Client client("pulsar://broker.example.com:6650/", config);

```

### Go client

To enable OAuth2 authentication in Go client, you need to configure OAuth2 authentication.
This example shows how to configure OAuth2 authentication in Go client. 

```go

oauth := pulsar.NewAuthenticationOAuth2(map[string]string{
		"type":       "client_credentials",
		"issuerUrl":  "https://dev-kt-aa9ne.us.auth0.com",
		"audience":   "https://dev-kt-aa9ne.us.auth0.com/api/v2/",
		"privateKey": "/path/to/privateKey",
		"clientId":   "0Xx...Yyxeny",
	})
client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:              "pulsar://my-cluster:6650",
		Authentication:   oauth,
})

```

### Python client

To enable OAuth2 authentication in Python client, you need to configure OAuth2 authentication.
This example shows how to configure OAuth2 authentication in Python client.

```python

from pulsar import Client, AuthenticationOauth2

params = '''
{
    "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
    "private_key": "/path/to/privateKey",
    "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
}
'''

client = Client("pulsar://my-cluster:6650", authentication=AuthenticationOauth2(params))

```

## CLI configuration

This section describes how to use Pulsar CLI tools to connect a cluster through OAuth2 authentication plugin.

### pulsar-admin

This example shows how to use pulsar-admin to connect to a cluster through OAuth2 authentication plugin.

```shell script

bin/pulsar-admin --admin-url https://streamnative.cloud:443 \
--auth-plugin org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2 \
--auth-params '{"privateKey":"file:///path/to/key/file.json",
    "issuerUrl":"https://dev-kt-aa9ne.us.auth0.com",
    "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"}' \
tenants list

```

Set the `admin-url` parameter to the Web service URL. A Web service URLis a combination of the protocol, hostname and port ID, such as `pulsar://localhost:6650`.
Set the `privateKey`, `issuerUrl`, and `audience` parameters to the values based on the configuration in the key file. For details, see [authentication types](#authentication-types).

### pulsar-client

This example shows how to use pulsar-client to connect to a cluster through OAuth2 authentication plugin.

```shell script

bin/pulsar-client \
--url SERVICE_URL \
--auth-plugin org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2 \
--auth-params '{"privateKey":"file:///path/to/key/file.json",
    "issuerUrl":"https://dev-kt-aa9ne.us.auth0.com",
    "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"}' \
produce test-topic -m "test-message" -n 10

```

Set the `admin-url` parameter to the Web service URL. A Web service URLis a combination of the protocol, hostname and port ID, such as `pulsar://localhost:6650`.
Set the `privateKey`, `issuerUrl`, and `audience` parameters to the values based on the configuration in the key file. For details, see [authentication types](#authentication-types).

### pulsar-perf

This example shows how to use pulsar-perf to connect to a cluster through OAuth2 authentication plugin.

```shell script

bin/pulsar-perf produce --service-url pulsar+ssl://streamnative.cloud:6651 \
--auth-plugin org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2 \
--auth-params '{"privateKey":"file:///path/to/key/file.json",
    "issuerUrl":"https://dev-kt-aa9ne.us.auth0.com",
    "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"}' \
-r 1000 -s 1024 test-topic

```

Set the `admin-url` parameter to the Web service URL. A Web service URLis a combination of the protocol, hostname and port ID, such as `pulsar://localhost:6650`.
Set the `privateKey`, `issuerUrl`, and `audience` parameters to the values based on the configuration in the key file. For details, see [authentication types](#authentication-types).
