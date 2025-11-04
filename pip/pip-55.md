# PIP-55: Refresh Authentication Credentials

* **Status**: Proposal
* **Author**: Matteo Merli
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:


## Goals

Enhance the Pulsar Authentication framework to support credentials that
expire over time and need to be refreshed by forcing clients to
re-authenticate.

Typical examples are:
 * TLS certificates with expiry date
 * JWT with expiry setting
 * Other token based systems for which the expiration might be retrieved
 * Handle revocation of credentials by forcing revalidation with time-bound limits

## Context

Currently, we're validating the authentication credentials when the connection is
established and that point we check the expiry times (eg: on TLS certificates or
on JWT tokens).

After the initial connection is authenticated, we store the "principal" which
will be used for authorization though the connection will not be re-authenticated
again.

If the token expires while the client is connected, we need to be able to
force the client to reconnect, in the least intrusive way, and disconnect if
it's the re-authentication fails.

## Implementation

The implementation is extending the work done to support mutual authentication
for SASL.

The single `AuthenticationState` interface credentials holder will have 2 more
methods:

```java
public interface AuthenticationState {
    // .....

    /**
     * If the authentication state is expired, it will force the connection
     * to be re-authenticated.
     */
    default boolean isExpired() {
        return false;
    }

    /**
     * If the authentication state supports refreshing and the credentials are expired,
     * the auth provider will call this method of initiate the refresh process.
     * <p>
     * The auth state here will return the broker side data that will be used to send
     * a challenge to the client.
     *
     * @return the {@link AuthData} for the broker challenge to client
     * @throws AuthenticationException
     */
    default AuthData refreshAuthentication() throws AuthenticationException {
        return null;
    }
}
```

Existing authentication plugins will be unaffected. If a new plugin wants
to support expiration it will just have to override the `isExpired()` method.

Pulsar broker will make sure to periodically check the expiration status
for the `AuthenticationState` of every `ServerCnx` object.

A new broker setting will be used to control the frequency of the expiration
check:

```shell
# Interval of time for checking for expired authentication credentials
authenticationRefreshCheckSeconds=60
```

### Interaction with older clients

Broker will be able to know whether a particular client supports the
authentication refresh feature.

If a client is not supporting the refreshing of the authentication and the
credentials are expired, the broker will disconnect it.

This will not be problematic because the client already will need a way to
pass the new credentials or it will fail anyway at the first time a TCP
connection with a broker is cut.
