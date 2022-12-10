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

## Enable TLS authentication on brokers/proxies

To configure brokers/proxies to authenticate clients using Mutual TLS, add the following parameters to the `conf/broker.conf` and the `conf/proxy.conf` file. If you use a standalone Pulsar, you need to add these parameters to the `conf/standalone.conf` file:

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls

brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters={"tlsCertFile":"/path/to/admin.cert.pem","tlsKeyFile":"/path/to/admin.key-pk8.pem"}
brokerClientTrustCertsFilePath=/path/to/ca.cert.pem

tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem

tlsRequireTrustedClientCertOnConnect=true
tlsAllowInsecureConnection=false

# Tls cert refresh duration in seconds (set 0 to check on every new connection)
tlsCertRefreshCheckDurationSec=300
```

## Configure TLS authentication in Pulsar clients

When using TLS authentication, clients connect via TLS transport. You need to configure clients to use `https://` and the `8443` port for the web service URL, use `pulsar+ssl://` and the `6651` port for the broker service URL.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"},{"label":"Go","value":"Go"},{"label":"C#","value":"C#"}]}>
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
<TabItem value="Go">

```go
client, err := pulsar.NewClient(ClientOptions{
		URL:                   "pulsar+ssl://broker.example.com:6651/",
		TLSTrustCertsFilePath: "/path/to/ca.cert.pem",
		Authentication:        pulsar.NewAuthenticationTLS("/path/to/my-role.cert.pem", "/path/to/my-role.key-pk8.pem"),
	})
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

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](/tools/pulsar-admin/), [`pulsar-perf`](reference-cli-tools.md), and [`pulsar-client`](reference-cli-tools.md) use the `conf/client.conf` config file in a Pulsar installation.

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

tlsRequireTrustedClientCertOnConnect=true
tlsAllowInsecureConnection=false
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
