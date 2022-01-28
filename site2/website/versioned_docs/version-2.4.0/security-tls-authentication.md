---
id: version-2.4.0-security-tls-authentication
title: Authentication using TLS
sidebar_label: Authentication using TLS
original_id: security-tls-authentication
---

## TLS Authentication Overview

TLS authentication is an extension of [TLS transport encryption](security-tls-transport.md), but instead of only servers having keys and certs which the client uses to verify the server's identity, clients also have keys and certs which the server uses to verify the client's identity. You must have TLS transport encryption configured on your cluster before you can use TLS authentication. This guide assumes you already have TLS transport encryption configured.

### Creating client certificates

Client certificates are generated using the same certificate authority as was used to generate the server certificates.

The biggest difference between client certs and server certs is that the **common name** for the client certificate is the **role token** which that client will be authenticated as.

First generate the key.
```bash
$ openssl genrsa -out admin.key.pem 2048
```

Similar to the broker, the client expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so convert it.

```bash
$ openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in admin.key.pem -out admin.key-pk8.pem -nocrypt
```

Generate the certificate request. When asked for a **common name**, enter the **role token** which you want this key pair to authenticate a client as.

```bash
$ openssl req -config openssl.cnf \
      -key admin.key.pem -new -sha256 -out admin.csr.pem
```
> Note
> If there is no openssl.cnf, please read [Certificate authority](http://pulsar.apache.org/docs/en/security-tls-transport/#certificate-authority) to get the openssl.cnf.

Sign with request with the certificate authority. Note that that client certs uses the **usr_cert** extension, which allows the cert to be used for client authentication.

```bash
$ openssl ca -config openssl.cnf -extensions usr_cert \
      -days 1000 -notext -md sha256 \
      -in admin.csr.pem -out admin.cert.pem
```

This will give you a cert, `admin.cert.pem`, and a key, `admin.key-pk8.pem`, which, with `ca.cert.pem`, can be used by clients to authenticate themselves to brokers and proxies as the role token ``admin``.

> Note
> If got "unable to load CA private key" error and the reason is "No such file or directory: /etc/pki/CA/private/cakey.pem" in this step. Please try :
>
> ```bash
> $ cd /etc/pki/tls/misc/CA
> $ ./CA -newca
> ```
>
> to generate `cakey.pem` .

## Enabling TLS Authentication ...

### ... on Brokers

To configure brokers to authenticate clients, put the following in `broker.conf`, alongside [the configuration to enable tls transport](security-tls-transport.md#broker-configuration):

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls

# operations and publish/consume from all topics
superUserRoles=admin

# Authentication settings of the broker itself. Used when the broker connects to other brokers, either in same or other clusters
brokerClientTlsEnabled=true
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters=tlsCertFile:/path/my-ca/admin.cert.pem,tlsKeyFile:/path/my-ca/admin.key-pk8.pem
brokerClientTrustCertsFilePath=/path/my-ca/certs/ca.cert.pem
```

### ... on Proxies

To configure proxies to authenticate clients, put the following in `proxy.conf`, alongside [the configuration to enable tls transport](security-tls-transport.md#proxy-configuration):

The proxy should have its own client key pair for connecting to brokers. The role token for this key pair should be configured in the ``proxyRoles`` of the brokers. See the [authorization guide](security-authorization.md) for more details.

```properties
# For clients connecting to the proxy
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls

# For the proxy to connect to brokers
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters=tlsCertFile:/path/to/proxy.cert.pem,tlsKeyFile:/path/to/proxy.key-pk8.pem
```

## Client configuration

When TLS authentication, the client needs to connect via TLS transport, so you need to configure the client to use ```https://``` and port 8443 for the web service URL, and ```pulsar+ssl://``` and port 6651 for the broker service URL.

### CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-pulsar-admin.md), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You'll need to add the following parameters to that file to use TLS authentication with Pulsar's CLI tools:

```properties
webServiceUrl=https://broker.example.com:8443/
brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/ca.cert.pem
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
authParams=tlsCertFile:/path/to/my-role.cert.pem,tlsKeyFile:/path/to/my-role.key-pk8.pem
```

### Java client

```java
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar+ssl://broker.example.com:6651/")
    .enableTls(true)
    .tlsTrustCertsFilePath("/path/to/ca.cert.pem")
    .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls",
                    "tlsCertFile:/path/to/my-role.cert.pem,tlsKeyFile:/path/to/my-role.key-pk8.pem")
    .build();
```

### Python client

```python
from pulsar import Client, AuthenticationTLS

auth = AuthenticationTLS("/path/to/my-role.cert.pem", "/path/to/my-role.key-pk8.pem")
client = Client("pulsar+ssl://broker.example.com:6651/",
                tls_trust_certs_file_path="/path/to/ca.cert.pem",
                tls_allow_insecure_connection=False,
				authentication=auth)
```

### C++ client

```c++
#include <pulsar/Client.h>

pulsar::ClientConfiguration config;
config.setUseTls(true);
config.setTlsTrustCertsFilePath("/path/to/ca.cert.pem");
config.setTlsAllowInsecureConnection(false);

pulsar::AuthenticationPtr auth = pulsar::AuthTls::create("/path/to/my-role.cert.pem",
                                                         "/path/to/my-role.key-pk8.pem")
config.setAuth(auth);

pulsar::Client client("pulsar+ssl://broker.example.com:6651/", config);
```

