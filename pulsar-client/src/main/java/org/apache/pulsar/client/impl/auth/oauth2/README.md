<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pulsar Client Authentication Plugin for OAuth 2.0

Pulsar supports authenticating clients using OAuth 2.0 access tokens.

You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that is permitted
to do some actions (eg: publish to a topic or consume from a topic). 

This module is to support Pulsar Client Authentication Plugin for OAuth 2.0. And after communicate with Oauth 2.0 server, 
client will get an `access token` from Oauth 2.0 server, and will pass this `access token` to Pulsar broker to do the authentication.
So the Broker side could still use `org.apache.pulsar.broker.authentication.AuthenticationProviderToken`,
also user can add their own `AuthenticationProvider` to work with this module.

## Provider Configuration
This library allows you to authenticate using an access token that is obtained from an OAuth 2.0 authorization service,
which acts as a _token issuer_.

### Authentication Types
The authentication type determines how to obtain an access token via an OAuth 2.0 authorization flow.

#### Client Credentials
The following parameters are supported:

| Parameter  | Description  | Example |
|---|---|---|
| `type` | Oauth 2.0 auth type. Optional. | default: `client_credentials`  |
| `issuerUrl` | URL of the provider which allows Pulsar to obtain an access token. Required. | `https://accounts.google.com` |
| `privateKey` | URL to a JSON credentials file (in JSON format; see below). Required. | See "Supported Pattern Formats" |
| `audience`  | An OAuth 2.0 "resource server" identifier for the Pulsar cluster. Required by some Identity Providers. Optional for client. | `https://broker.example.com` |

### Supported Pattern Formats of `privateKey`
The `privateKey` parameter supports the following three pattern formats, and contains client Credentials:

- `file:///path/to/file`
- `file:/path/to/file`
- `data:application/json;base64,<base64-encoded value>`

The credentials file contains service account credentials for use with the Client Credentials authentication type.

For example of a credentials file `credentials_file.json`:
```json
{
  "type": "client_credentials",
  "client_id": "d9ZyX97q1ef8Cr81WHVC4hFQ64vSlDK3",
  "client_secret": "on1uJ...k6F6R",
  "client_email": "1234567890-abcdefghijklmnopqrstuvwxyz@developer.gserviceaccount.com",
  "issuer_url": "https://accounts.google.com"
}
```

The default type is `client_credentials`, and for this type, fields "client_id" and "client_secret" is required.

### Example for a typical original Oauth2 request mapping

A typical original Oauth2 request, which used to get access token from Oauth2 server, is like this: 

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

In which,
- `issuerUrl` parameter in this plugin is mapped to `--url https://dev-kt-aa9ne.us.auth0.com`
- `privateKey` file parameter in this plugin should at least contains fields `client_id` and `client_secret`.
- `audience` parameter in this plugin is mapped to  `"audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"`. This field is only used by some identity providers.

## Pulsar Client Config
You can use the provider with the following Pulsar clients.

### Java
You can use the factory method:
```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactoryOAuth2.clientCredentials(this.issuerUrl, this.credentialsUrl, this.audience))
    .build();
```

Similarly, you can use encoded parameters:
```java
Authentication auth = AuthenticationFactory
    .create(AuthenticationOAuth2.class.getName(), "{"type":"client_credentials","privateKey":"...","issuerUrl":"...","audience":"..."}");

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(auth)
    .build();
```
