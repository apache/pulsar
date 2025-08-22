# PIP-25: Token based authentication

* **Status**: Done
* **Author**: [Matteo Merli](https://github.com/merlimat)
* **Pull Request**: [#2888](https://github.com/apache/pulsar/pull/2888)
* **Mailing List discussion**:
* **Release**: 2.3.0


## Motivation

Pulsar has a [pluggable authentication mechanism](http://pulsar.apache.org/docs/en/security-extending/#authentication)
that currently supports 3 auth providers.

 1. TLS certificates
 2. [Athenz](http://www.athenz.io/)
 3. [Basic access authentication](https://en.wikipedia.org/wiki/Basic_access_authentication)

Each of them has few issues which could be summarized as:

 1. TLS
   * Requires to have multiple key files in both clients and brokers, making it
     difficult to distribute the credentials.
   * The tools to generate keys are hard to use (OpenSSL)
   * It is hard to automate the creation of certificates
   * Manage the Certificate authority certificate is even harder

 2. Athenz
   * Requires additional service to run
   * Also targets authorization which we don't need since we have internal implementation
     for that

 3. Basic auth
   * Very minimal password file based authentication, not really usable when there
     are multiple clients/brokers

To address these issues, this proposal plans to add a new auth provider that uses
[JSON Web Tokens](https://jwt.io/introduction/)
 ([RFC-7519](https://tools.ietf.org/html/rfc7519)).

 The compact representation of a signed JWT is a string that has three
 parts, each separated by a `.`:

```
 eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY
```

The 3 parts are:
 1. **Header**: contains token infos such as the which algorithm is used for
    the signature
 2. **Payload**: Application specific information. Examples are: `subject`
    (user identifier or principal), `expiration`, etc.. Any kind of info
    can be attached when creating a token and used later during the
    validation
 3. **Signature**: Crypto signature that ensures authenticity of the
    token    

The main properties of JSON Web Tokens are:
  1. Tokens are created and signed with a secret key
  2. The secret key can either be a single key or a pair of public/private
     keys. If 2 keys are used, the private key is used to generate the
     tokens and the public key can be distributed to all servers to
     perform validation of such tokens.
  3. Each server can validate a token without talking to any external
     service
  4. By adding information in the payload, the tokens can be scoped in
     many ways. eg:
       * Expire time
       * Limited scope by including reference to some resource
       * Restrict to some client IPs
  5. Revocations are not directly supported, but rather need to be
     implemented by maintaining a black-list of revoked tokens

Note: the TCP connection between client and broker is still expected
to be protected by TLS encryption. That is because the token shouldn't
be sent in clear text over the wire.

## Changes

### Token generation

We need to provide tools to allow an administrator to create the secret
key and the tokens for the users.

For example:

```shell
pulsar tokens create --key $SECRET_KEY --subject new-user-id
```

This will generate a new token and print it on console. This will be done
by administrator (or some automated service) and the token will be passed
to client.

Similarly, administrator will be able to create the secret key to
bootstrap the tokens generation:

```shell
pulsar tokens create-secret-key
```

### Client Side

From client library perspective, a new AuthenticationProvider will be
added that will support taking token and pass that directly to broker
on connection. Client plugin will not interpret the token in any form,
rather just treat it as an opaque string.

This will ensure that multiple tokens format can be used if required.

Additionally the `AuthenticationProvider` will allow the application
to pass a `Supplier<String>` to give the opportunity to fetch the
token from some config or secret store.

### Broker side

AuthenticationProvider in Broker will receive the token and validate
that with the secret key.

The secret key will be either provided in `broker.conf` or a special
class implementing `Supplier<String>` will be specified to fetch the
secret key from config or secret store.
