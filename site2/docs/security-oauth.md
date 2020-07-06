---
id: security-oauth
title: Client authentication using OAuth 2.0 access tokens
sidebar_label: Authentication using OAuth 2.0 access tokens
---

Pulsar supports authenticating clients using OAuth 2.0 access tokens. You can use OAuth 2.0 access tokens to identify a Pulsar client and associate the Pulsar client with some "principal" (or "role"), which is permitted to do some actions, such as publishing messages to a topic or consume messages from a topic.

This module is used to support the Pulsar client authentication plugin for OAuth 2.0. After communicating with the Oauth 2.0 server, the Pulsar client gets an `access token` from the Oauth 2.0 server, and passes this `access token` to the Pulsar broker to do the authentication. The broker can use the `org.apache.pulsar.broker.authentication.AuthenticationProviderToken`. Or, you can add your own `AuthenticationProvider` to make it with this module.

## Configure authentication provider

This library allows you to authenticate the Pulsar client by using an access token that is obtained from an OAuth 2.0 authorization service, which acts as a _token issuer_.

### Authentication types

The authentication type determines how to obtain an access token through an OAuth 2.0 authorization flow.

#### Note
> Currently, the Pulsar Java client only supports `client_credentials` .

#### Client credentials

The following table lists parameters supported for `client credentials`.

| Parameter | Description | Example | Required or not |
| --- | --- | --- | --- |
| `type` | Oauth 2.0 authentication type. |  `client_credentials` (default) | Optional |
| `issuerUrl` | URL of the authentication provider which allows the Pulsar client to obtain an access token | `https://accounts.google.com` | Required |
| `privateKey` | URL to a JSON credentials file  | See [supported pattern formats](#supported-pattern-formats-of-privatekey). | Required |
| `audience`  | An OAuth 2.0 "resource server" identifier for the Pulsar cluster | `https://broker.example.com` | Required |

### Supported Pattern Formats of `privateKey`

The `privateKey` parameter supports the following three pattern formats, and contains client Credentials.

- `file:///path/to/file`
- `file:/path/to/file`
- `data:application/json;base64,<base64-encoded value>`

The credentials file contains service account credentials for use with the client credentials authentication type.

The following shows an example of a credentials file `credentials_file.json`.

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

A typical original Oauth2 request, which is used to obtain the access token from Oauth2 server, is like this: 

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

- `issuerUrl` parameter in this plugin is mapped to `--url https://dev-kt-aa9ne.us.auth0.com/oauth/token`
- `privateKey` file parameter in this plugin should at least contains fields `client_id` and `client_secret`.
- `audience` parameter in this plugin is mapped to  `"audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"`

## Pulsar client configuration

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