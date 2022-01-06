---
id: version-2.7.4-security-tls-authentication
title: Authentication using TLS
sidebar_label: Authentication using TLS
original_id: security-tls-authentication
---

## TLS authentication overview

TLS authentication is an extension of [TLS transport encryption](security-tls-transport.md). Not only servers have keys and certs that the client uses to verify the identity of servers, clients also have keys and certs that the server uses to verify the identity of clients. You must have TLS transport encryption configured on your cluster before you can use TLS authentication. This guide assumes you already have TLS transport encryption configured.

`Bouncy Castle Provider` provides TLS related cipher suites and algorithms in Pulsar. If you need [FIPS](https://www.bouncycastle.org/fips_faq.html) version of `Bouncy Castle Provider`, please reference [Bouncy Castle page](security-bouncy-castle.md).

### Create client certificates

Client certificates are generated using the certificate authority. Server certificates are also generated with the same certificate authority.

The biggest difference between client certs and server certs is that the **common name** for the client certificate is the **role token** which that client is authenticated as.

First, you need to enter the following command to generate the key :

```bash
$ openssl genrsa -out admin.key.pem 2048
```

Similar to the broker, the client expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so you need to convert it by entering the following command:

```bash
$ openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in admin.key.pem -out admin.key-pk8.pem -nocrypt
```

Next, enter the command below to generate the certificate request. When you are asked for a **common name**, enter the **role token** that you want this key pair to authenticate a client as.

```bash
$ openssl req -config openssl.cnf \
      -key admin.key.pem -new -sha256 -out admin.csr.pem
```
> Note
> If openssl.cnf is not specified, read [Certificate authority](http://pulsar.apache.org/docs/en/security-tls-transport/#certificate-authority) to get the openssl.cnf.

Then, enter the command below to sign with request with the certificate authority. Note that the client certs uses the **usr_cert** extension, which allows the cert to be used for client authentication.

```bash
$ openssl ca -config openssl.cnf -extensions usr_cert \
      -days 1000 -notext -md sha256 \
      -in admin.csr.pem -out admin.cert.pem
```

You can get a cert, `admin.cert.pem`, and a key, `admin.key-pk8.pem` from this command. With `ca.cert.pem`, clients can use this cert and this key to authenticate themselves to brokers and proxies as the role token ``admin``.

> Note
> If the "unable to load CA private key" error occurs and the reason of this error is "No such file or directory: /etc/pki/CA/private/cakey.pem" in this step. Try the command below:
>
> ```bash
> $ cd /etc/pki/tls/misc/CA
> $ ./CA -newca
> ```
>
> to generate `cakey.pem` .

## Enable TLS authentication on brokers

To configure brokers to authenticate clients, add the following parameters to `broker.conf`, alongside [the configuration to enable tls transport](security-tls-transport.md#broker-configuration):

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls

# operations and publish/consume from all topics
superUserRoles=admin

# Authentication settings of the broker itself. Used when the broker connects to other brokers, either in same or other clusters
brokerClientTlsEnabled=true
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters={"tlsCertFile":"/path/my-ca/admin.cert.pem","tlsKeyFile":"/path/my-ca/admin.key-pk8.pem"}
brokerClientTrustCertsFilePath=/path/my-ca/certs/ca.cert.pem
```

## Enable TLS authentication on proxies

To configure proxies to authenticate clients, add the following parameters to `proxy.conf`, alongside [the configuration to enable tls transport](security-tls-transport.md#proxy-configuration):

The proxy should have its own client key pair for connecting to brokers. You need to configure the role token for this key pair in the ``proxyRoles`` of the brokers. See the [authorization guide](security-authorization.md) for more details.

```properties
# For clients connecting to the proxy
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls

# For the proxy to connect to brokers
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters=tlsCertFile:/path/to/proxy.cert.pem,tlsKeyFile:/path/to/proxy.key-pk8.pem
```

## Client configuration

When you use TLS authentication, client connects via TLS transport. You need to configure the client to use ```https://``` and 8443 port for the web service URL, ```pulsar+ssl://``` and 6651 port for the broker service URL.

### CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-pulsar-admin.md), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You need to add the following parameters to that file to use TLS authentication with the CLI tools of Pulsar:

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

### Node.js client

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  const auth = new Pulsar.AuthenticationTls({
    certificatePath: '/path/to/my-role.cert.pem',
    privateKeyPath: '/path/to/my-role.key-pk8.pem',
  });

  const client = new Pulsar.Client({
    serviceUrl: 'pulsar+ssl://broker.example.com:6651/',
    authentication: auth,
    tlsTrustCertsFilePath: '/path/to/ca.cert.pem',
  });
})();
```

### C# client

```c#
var clientCertificate = new X509Certificate2("admin.pfx");
var client = PulsarClient.Builder()
                         .AuthenticateUsingClientCertificate(clientCertificate)
                         .Build();
```
