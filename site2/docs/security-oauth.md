---
id: security-oauth
title: Client authentication using OAuth 2.0 access tokens
sidebar_label: Authentication using OAuth 2.0 access tokens
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
| `privateKey` | URL to a JSON credentials file  | Support the following pattern formats: <br> <li> `file:///path/to/file` <li>`file:/path/to/file` <li> `data:application/json;base64,<base64-encoded value>` | Required |
| `audience`  | An OAuth 2.0 "resource server" identifier for the Pulsar cluster | `https://broker.example.com` | Required |

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

### Typical original Oauth2 request mapping

The following shows a typical original Oauth2 request, which is used to obtain the access token from the Oauth2 server.

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

In the above example, the mapping relationship is shown as below.

- The `issuerUrl` parameter in this plugin is mapped to `--url https://dev-kt-aa9ne.us.auth0.com/oauth/token`.
- The `privateKey` file parameter in this plugin should at least contains the `client_id` and `client_secret` fields.
- The `audience` parameter in this plugin is mapped to  `"audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/"`.

## Client Configuration

You can use the Oauth2 authentication provider with the following Pulsar clients.

### Java

You can use the factory method to configure authentication for Pulsar Java client.

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactoryOAuth2.clientCredentials(this.issuerUrl, this.credentialsUrl, this.audience))
    .build();
```

In addition, you can also use the encoded parameters to configure authentication for Pulsar Java client.

```java
Authentication auth = AuthenticationFactory
    .create(AuthenticationOAuth2.class.getName(), "{"type":"client_credentials","privateKey":"...","issuerUrl":"...","audience":"..."}");
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(auth)
    .build();
```