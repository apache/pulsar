---
id: security-tls-authentication
title: Authentication using TLS
sidebar_label: "Authentication using TLS"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

## TLS authentication overview

TLS authentication is an extension of [TLS transport encryption](security-tls-transport.md). Not only servers have keys and certs that the client uses to verify the identity of servers, clients also have keys and certs that the server uses to verify the identity of clients. You must have TLS transport encryption configured on your cluster before you can use TLS authentication. This guide assumes you already have TLS transport encryption configured.

## Create client certificates

Client certificates are generated using the certificate authority. Server certificates are also generated with the same certificate authority.

The biggest difference between client certs and server certs is that the **common name** for the client certificate is the **role token** that the client is authenticated as.

To use client certificates, you need to set `tlsRequireTrustedClientCertOnConnect=true` at the broker side. For details, refer to [TLS broker configuration](security-tls-transport.md#configure-brokers).

First, you need to enter the following command to generate the key :

```bash
openssl genrsa -out admin.key.pem 2048
```

Similar to the broker, the client expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so you need to convert it by entering the following command:

```bash
openssl pkcs8 -topk8 -inform PEM -outform PEM \
    -in admin.key.pem -out admin.key-pk8.pem -nocrypt
```

Next, enter the command below to generate the certificate request. When you are asked for a **common name**, enter the **role token** that you want this key pair to authenticate a client as.

```bash
openssl req -config openssl.cnf \
    -key admin.key.pem -new -sha256 -out admin.csr.pem
```

:::note

If `openssl.cnf` is not specified, read [Certificate authority](security-tls-transport.md#certificate-authority) to get `openssl.cnf`.

:::

Then, enter the command below to sign with a request with the certificate authority. Note that the client certs use the **usr_cert** extension, which allows the cert to be used for client authentication.

```bash
openssl ca -config openssl.cnf -extensions usr_cert \
    -days 1000 -notext -md sha256 \
    -in admin.csr.pem -out admin.cert.pem
```

You can get a cert, `admin.cert.pem`, and a key, `admin.key-pk8.pem` from this command. With `ca.cert.pem`, clients can use this cert and this key to authenticate themselves to brokers and proxies as the role token ``admin``.

:::note

If the "unable to load CA private key" error occurs and the reason for this error is "No such file or directory: /etc/pki/CA/private/cakey.pem" in this step. Try the command below to generate `cakey.pem`.

```bash
cd /etc/pki/tls/misc/CA
./CA -newca
```

:::

## Enable TLS authentication on brokers

To configure brokers to authenticate clients, add the following parameters to `broker.conf`, alongside [the configuration to enable TLS transport](security-tls-transport.md#configure-brokers):

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls
tlsRequireTrustedClientCertOnConnect=true

brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters={"tlsCertFile":"/path/my-ca/admin.cert.pem","tlsKeyFile":"/path/my-ca/admin.key-pk8.pem"}
brokerClientTrustCertsFilePath=/path/my-ca/certs/ca.cert.pem
```

## Enable TLS authentication on proxies

To configure proxies to authenticate clients, add the following parameters to `proxy.conf`, alongside [the configuration to enable TLS transport](security-tls-transport.md#configure-proxies):

The proxy should have its own client key pair for connecting to brokers. You need to configure the role token for this key pair in the `proxyRoles` of the brokers. See the [authorization guide](security-authorization.md) for more details.

```properties
# For clients connecting to the proxy
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls
tlsRequireTrustedClientCertOnConnect=true

# For the proxy to connect to brokers
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters=tlsCertFile:/path/to/proxy.cert.pem,tlsKeyFile:/path/to/proxy.key-pk8.pem
```

## Configure TLS authentication in Pulsar clients

When you use TLS authentication, client connects via TLS transport. You need to configure the client to use `https://` and 8443 port for the web service URL, `pulsar+ssl://` and 6651 port for the broker service URL.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"},{"label":"C#","value":"C#"}]}>
<TabItem value="Java">

```java
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar+ssl://broker.example.com:6651/")
    .tlsTrustCertsFilePath("/path/to/ca.cert.pem")
    .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls",
                    "tlsCertFile:/path/to/my-role.cert.pem,tlsKeyFile:/path/to/my-role.key-pk8.pem")
    .build();
```

</TabItem>
<TabItem value="Python">

```python
from pulsar import Client, AuthenticationTLS

auth = AuthenticationTLS("/path/to/my-role.cert.pem", "/path/to/my-role.key-pk8.pem")
client = Client("pulsar+ssl://broker.example.com:6651/",
                tls_trust_certs_file_path="/path/to/ca.cert.pem",
                tls_allow_insecure_connection=False,
				authentication=auth)
```

</TabItem>
<TabItem value="C++">

```cpp
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

</TabItem>
<TabItem value="Node.js">

```javascript
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

</TabItem>
<TabItem value="C#">

```csharp
var clientCertificate = new X509Certificate2("admin.pfx");
var client = PulsarClient.Builder()
                         .AuthenticateUsingClientCertificate(clientCertificate)
                         .Build();
```

</TabItem>
</Tabs>
````

## Configure TLS authentication in CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](/tools/pulsar-admin/), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

To use TLS authentication with the CLI tools of Pulsar, you need to add the following parameters to the `conf/client.conf` file, alongside [the configuration to enable TLS encryption](security-tls-transport.md#configure-tls-encryption-in-cli-tools):

```properties
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
authParams=tlsCertFile:/path/to/my-role.cert.pem,tlsKeyFile:/path/to/my-role.key-pk8.pem
```

## Configure TLS authentication with KeyStore 

Apache Pulsar supports [TLS encryption](security-tls-transport.md) and [TLS authentication](security-tls-authentication.md) between clients and Apache Pulsar service. By default, it uses PEM format file configuration. This section tries to describe how to use [KeyStore](https://en.wikipedia.org/wiki/Java_KeyStore) type to configure TLS.

### Configure brokers

Configure the `broker.conf` file as follows.

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls

# Enable KeyStore type
tlsEnabledWithKeyStore=true
tlsRequireTrustedClientCertOnConnect=true

# key store
tlsKeyStoreType=JKS
tlsKeyStore=/var/private/tls/broker.keystore.jks
tlsKeyStorePassword=brokerpw

# trust store
tlsTrustStoreType=JKS
tlsTrustStore=/var/private/tls/broker.truststore.jks
tlsTrustStorePassword=brokerpw

# internal client/admin-client config
brokerClientTlsEnabled=true
brokerClientTlsEnabledWithKeyStore=true
brokerClientTlsTrustStoreType=JKS
brokerClientTlsTrustStore=/var/private/tls/client.truststore.jks
brokerClientTlsTrustStorePassword=clientpw
# internal auth config
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls
brokerClientAuthenticationParameters={"keyStoreType":"JKS","keyStorePath":"/var/private/tls/client.keystore.jks","keyStorePassword":"clientpw"}
```

### Configure clients

Besides configuring [TLS encryption](security-tls-transport.md), you need to configure the KeyStore, which contains a valid CN as client role, for clients.

For example:

1. for [Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools#pulsar-admin), [`pulsar-perf`](reference-cli-tools#pulsar-perf), and [`pulsar-client`](reference-cli-tools#pulsar-client), set the `conf/client.conf` file in a Pulsar installation.

   ```properties
   webServiceUrl=https://broker.example.com:8443/
   brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
   useKeyStoreTls=true
   tlsTrustStoreType=JKS
   tlsTrustStorePath=/var/private/tls/client.truststore.jks
   tlsTrustStorePassword=clientpw
   authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls
   authParams={"keyStoreType":"JKS","keyStorePath":"/var/private/tls/client.keystore.jks","keyStorePassword":"clientpw"}
   ```

1. for Java client

   ```java
   import org.apache.pulsar.client.api.PulsarClient;

   PulsarClient client = PulsarClient.builder()
       .serviceUrl("pulsar+ssl://broker.example.com:6651/")
       .useKeyStoreTls(true)
       .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
       .tlsTrustStorePassword("clientpw")
       .allowTlsInsecureConnection(false)
       .enableTlsHostnameVerification(false)
       .authentication(
               "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls",
               "keyStoreType:JKS,keyStorePath:/var/private/tls/client.keystore.jks,keyStorePassword:clientpw")
       .build();
   ```

1. for Java admin client

   ```java
       PulsarAdmin amdin = PulsarAdmin.builder().serviceHttpUrl("https://broker.example.com:8443")
           .useKeyStoreTls(true)
           .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
           .tlsTrustStorePassword("clientpw")
           .allowTlsInsecureConnection(false)
           .enableTlsHostnameVerification(false)
           .authentication(
                  "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls",
                  "keyStoreType:JKS,keyStorePath:/var/private/tls/client.keystore.jks,keyStorePassword:clientpw")
           .build();
   ```

:::note

Configure `tlsTrustStorePath` when you set `useKeyStoreTls` to `true`.

:::

## Enable TLS Logging

You can enable TLS debug logging at the JVM level by starting the brokers and/or clients with `javax.net.debug` system property. For example:

```shell
-Djavax.net.debug=all
```

You can find more details on this in [Oracle documentation](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html) on [debugging SSL/TLS connections](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html).
