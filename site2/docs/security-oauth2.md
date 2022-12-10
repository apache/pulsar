---
id: security-oauth2
title: Authentication using OAuth 2.0 access tokens
sidebar_label: "Authentication using OAuth 2.0 access tokens"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

Pulsar supports authenticating clients using OAuth 2.0 access tokens. Using an access token obtained from an OAuth 2.0 authorization service (acts as a token issuer), you can identify a Pulsar client and associate it with a "principal" (or "role") that is permitted to do some actions, such as publishing messages to a topic or consuming messages from a topic.

After communicating with the OAuth 2.0 server, the Pulsar client gets an access token from the server and passes this access token to brokers for authentication. By default, brokers can use the `org.apache.pulsar.broker.authentication.AuthenticationProviderToken`. Alternatively, you can customize the value of `AuthenticationProvider`.

## Enable OAuth2 authentication on brokers/proxies

To configure brokers/proxies to authenticate clients using OAuth2, add the following parameters to the `conf/broker.conf` and the `conf/proxy.conf` file. If you use a standalone Pulsar, you need to add these parameters to the `conf/standalone.conf` file:

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# Authentication settings of the broker itself. Used when the broker connects to other brokers, or when the proxy connects to brokers, either in same or other clusters
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2
brokerClientAuthenticationParameters={"privateKey":"file:///path/to/privateKey","audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/","issuerUrl":"https://dev-kt-aa9ne.us.auth0.com"}
# brokerClientAuthenticationParameters={"privateKey":"data:application/json;base64,privateKey-body-to-base64","audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/","issuerUrl":"https://dev-kt-aa9ne.us.auth0.com"}

# If using secret key (Note: key files must be DER-encoded)
tokenSecretKey=file:///path/to/secret.key
# The key can also be passed inline:
# tokenSecretKey=data:;base64,FLFyW0oLJ2Fi22KKCm21J18mbAdztfSHN/lAT5ucEKU=

# If using public/private (Note: key files must be DER-encoded)
# tokenPublicKey=file:///path/to/public.key
```

## Configure OAuth2 authentication in Pulsar clients

You can use the OAuth2 authentication provider with the following Pulsar clients.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"},{"label":"Go","value":"Go"}]}>
<TabItem value="Java">

```java
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

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

</TabItem>
<TabItem value="Python">

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

</TabItem>
<TabItem value="C++">

```cpp
#include <pulsar/Client.h>

pulsar::ClientConfiguration config;
std::string params = R"({
    "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
    "private_key": "../../pulsar-broker/src/test/resources/authentication/token/cpp_credentials_file.json",
    "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

config.setAuth(pulsar::AuthOauth2::create(params));

pulsar::Client client("pulsar://broker.example.com:6650/", config);
```

</TabItem>
<TabItem value="Node.js">

```javascript
    const Pulsar = require('pulsar-client');
    const issuer_url = process.env.ISSUER_URL;
    const private_key = process.env.PRIVATE_KEY;
    const audience = process.env.AUDIENCE;
    const scope = process.env.SCOPE;
    const service_url = process.env.SERVICE_URL;
    const client_id = process.env.CLIENT_ID;
    const client_secret = process.env.CLIENT_SECRET;
    (async () => {
      const params = {
        issuer_url: issuer_url
      }
      if (private_key.length > 0) {
        params['private_key'] = private_key
      } else {
        params['client_id'] = client_id
        params['client_secret'] = client_secret
      }
      if (audience.length > 0) {
        params['audience'] = audience
      }
      if (scope.length > 0) {
        params['scope'] = scope
      }
      const auth = new Pulsar.AuthenticationOauth2(params);
      // Create a client
      const client = new Pulsar.Client({
        serviceUrl: service_url,
        tlsAllowInsecureConnection: true,
        authentication: auth,
      });
      await client.close();
    })();
```

:::note

The support for OAuth2 authentication is only available in Node.js client 1.6.2 and later versions.

:::

</TabItem>
<TabItem value="Go">

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

</TabItem>
</Tabs>
````

## Configure OAuth2 authentication in CLI tools

This section describes how to use Pulsar CLI tools to connect a cluster through OAuth2 authentication plugin.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"pulsar-client","value":"pulsar-client"},{"label":"pulsar-perf","value":"pulsar-perf"}]}>
<TabItem value="pulsar-admin">

```shell
bin/pulsar-admin --admin-url https://streamnative.cloud:443 \
--auth-plugin org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2 \
--auth-params '{"privateKey":"file:///path/to/key/file.json",
    "issuerUrl":"https://dev-kt-aa9ne.us.auth0.com",
    "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"}' \
tenants list
```

</TabItem>
<TabItem value="pulsar-client">

```shell
bin/pulsar-client \
--url SERVICE_URL \
--auth-plugin org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2 \
--auth-params '{"privateKey":"file:///path/to/key/file.json",
    "issuerUrl":"https://dev-kt-aa9ne.us.auth0.com",
    "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"}' \
produce test-topic -m "test-message" -n 10
```

</TabItem>
<TabItem value="pulsar-perf">

```shell
bin/pulsar-perf produce --service-url pulsar+ssl://streamnative.cloud:6651 \
--auth-plugin org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2 \
--auth-params '{"privateKey":"file:///path/to/key/file.json",
    "issuerUrl":"https://dev-kt-aa9ne.us.auth0.com",
    "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"}' \
-r 1000 -s 1024 test-topic
```

</TabItem>
</Tabs>
````

* Set the `admin-url` parameter to the Web service URL. A Web service URL is a combination of the protocol, hostname and port ID, such as `pulsar://localhost:6650`.
* Set the `privateKey`, `issuerUrl`, and `audience` parameters to the values based on the configuration in the key file. For details, see [authentication types](#authentication-types).

#### Authentication types

Currently, Pulsar clients only support the `client_credentials` authentication type. The authentication type determines how to obtain an access token through an OAuth 2.0 authorization service.

The following table outlines the parameters of the `client_credentials` authentication type.

| Parameter | Description | Example | Required or not |
| --- | --- | --- | --- |
| `type` | OAuth 2.0 authentication type. |  `client_credentials` (default) | Optional |
| `issuerUrl` | The URL of the authentication provider which allows the Pulsar client to obtain an access token. | `https://accounts.google.com` | Required |
| `privateKey` | The URL to the JSON credentials file.  | Support the following pattern formats: <br /> <li> `file:///path/to/file` </li><li>`file:/path/to/file` </li><li> `data:application/json;base64,<base64-encoded value>` </li>| Required |
| `audience`  | The OAuth 2.0 "resource server" identifier for a Pulsar cluster. | `https://broker.example.com` | Optional |
| `scope` |  The scope of an access request. <br />For more information, see [access token scope](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3). | api://pulsar-cluster-1/.default | Optional |

The credentials file `credentials_file.json` contains the service account credentials used with the client authentication type. The following is an example of the credentials file. The authentication type is set to `client_credentials` by default. And the fields "client_id" and "client_secret" are required.

```json
{
  "type": "client_credentials",
  "client_id": "d9ZyX97q1ef8Cr81WHVC4hFQ64vSlDK3",
  "client_secret": "on1uJ...k6F6R",
  "client_email": "1234567890-abcdefghijklmnopqrstuvwxyz@developer.gserviceaccount.com",
  "issuer_url": "https://accounts.google.com"
}
```

The following is an example of a typical original OAuth2 request, which is used to obtain an access token from the OAuth2 server.

```bash
curl --request POST \
  --url https://dev-kt-aa9ne.us.auth0.com/oauth/token \
  --header 'content-type: application/json' \
  --data '{
  "client_id":"Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
  "client_secret":"rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb",
  "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/",
  "grant_type":"client_credentials"}'
```

In the above example, the mapping relationship is shown below.
- The `issuerUrl` parameter is mapped to `--url https://dev-kt-aa9ne.us.auth0.com`.
- The `privateKey` parameter should contain the `client_id` and `client_secret` fields at least.
- The `audience` parameter is mapped to  `"audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"`. This field is only used by some identity providers.
