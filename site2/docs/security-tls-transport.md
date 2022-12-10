---
id: security-tls-transport
title: TLS Encryption
sidebar_label: "TLS Encryption"
---


````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

## TLS overview

Transport Layer Security (TLS) is a form of [public key cryptography](https://en.wikipedia.org/wiki/Public-key_cryptography). By default, Pulsar clients communicate with Pulsar services in plain text. This means that all data is sent in the clear. You can use TLS to encrypt this traffic to protect the traffic from the snooping of a man-in-the-middle attacker.

This section introduces how to configure TLS encryption in Pulsar. For how to configure TLS authentication in Pulsar, refer to [TLS authentication](security-tls-authentication.md). Alternatively, you can use another [Athenz authentication](security-athenz.md) on top of TLS transport encryption.

:::note

Enabling TLS encryption may impact the performance due to encryption overhead.

:::

### TLS certificates

TLS certificates include the following three types. Each certificate (key pair) contains both a public key that encrypts messages and a private key that decrypts messages.
* Certificate Authority (CA)
  * CA private key is distributed to all parties involved.
  * CA public key (**trust cert**) is used for signing a certificate for either broker or clients.
* Server key pairs
* Client key pairs (for mutual TLS)

For both server and client certificates, the private key with a certificate request is generated first, and the public key (the certificate) is generated after the **trust cert** signs the certificate request. When [TLS authentication](security-tls-authentication.md) is enabled, the server uses the **trust cert** to verify that the client has a key pair that the certificate authority signs. The Common Name (CN) of a client certificate is used as the client's role token, while the Subject Alternative Name (SAN) of a server certificate is used for [Hostname verification](#hostname-verification).

:::note

The validity of these certificates is 365 days. It's highly recommended to use `sha256` or `sha512` as the signature algorithm, while `sha1` is not supported.

:::

### Certificate formats

You can use either one of the following certificate formats to configure TLS encryption:
* Recommended: Privacy Enhanced Mail (PEM). 
  See [Configure TLS encryption with PEM](#configure-tls-encryption-with-pem) for detailed instructions.
* Optional: Java [KeyStore](https://en.wikipedia.org/wiki/Java_KeyStore) (JKS). 
  See [Configure TLS encryption with KeyStore](#configure-tls-encryption-with-keystore) for detailed instructions.

### Hostname verification

Hostname verification is a TLS security feature whereby a client can refuse to connect to a server if the Subject Alternative Name (SAN) does not match the hostname that the hostname is connecting to. 

By default, Pulsar clients disable hostname verification, as it requires that each broker has a DNS record and a unique cert.

One scenario where you may want to enable hostname verification is where you have multiple proxy nodes behind a VIP, and the VIP has a DNS record, for example, `pulsar.mycompany.com`. In this case, you can generate a TLS cert with `pulsar.mycompany.com` as the SAN, and then enable hostname verification on the client.

To enable hostname verification in Pulsar, ensure that SAN exactly matches the fully qualified domain name (FQDN) of the server. The client compares the SAN with the DNS domain name to ensure that it is connecting to the desired server. See [Configure clients](security-tls-transport.md#configure-clients) for more details.

Moreover, as the administrator has full control of the CA, a bad actor is unlikely to be able to pull off a man-in-the-middle attack. `allowInsecureConnection` allows the client to connect to servers whose cert has not been signed by an approved CA. The client disables `allowInsecureConnection` by default, and you should always disable `allowInsecureConnection` in production environments. As long as you disable `allowInsecureConnection`, a man-in-the-middle attack requires that the attacker has access to the CA.

## Configure TLS encryption with PEM

By default, Pulsar uses [netty-tcnative](https://github.com/netty/netty-tcnative). It includes two implementations, `OpenSSL` (default) and `JDK`. When `OpenSSL` is unavailable, `JDK` is used.

### Create TLS certificates

Creating TLS certificates involves creating a [certificate authority](#create-a-certificate-authority), a [server certificate](#create-a-server-certificate), and a [client certificate](#create-a-client-certificate). 

#### Create a certificate authority

You can use a certificate authority (CA) to sign both server and client certificates. This ensures that each party trusts the others. Store CA in a very secure location (ideally completely disconnected from networks, air-gapped, and fully encrypted).

Use the following command to create a CA.

   ```bash
   openssl genrsa -out ca.key.pem 2048
   openssl req -x509 -new -nodes -key ca.key.pem -subj "/CN=CARoot" -days 365 -out ca.cert.pem
   ```

   :::note

   The default `openssl` on macOS doesn't work for the commands above. You need to upgrade `openssl` via Homebrew:

   ```bash
   brew install openssl
   export PATH="/usr/local/Cellar/openssl@3/3.0.1/bin:$PATH"
   ```

   Use the actual path from the output of the `brew install` command. Note that version number `3.0.1` might change. 

   :::

#### Create a server certificate

Once you have created a CA, you can create certificate requests and sign them with the CA.

1. Generate the server's private key.

   ```bash
   openssl genrsa -out broker.key.pem 2048
   ```

   The broker expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format. Enter the following command to convert it.

   ```bash
   openssl pkcs8 -topk8 -inform PEM -outform PEM -in broker.key.pem -out broker.key-pk8.pem -nocrypt
   ```

2. Create a `broker.conf` file with the following content:

   ```properties
   [ req ]
   default_bits = 2048
   prompt = no
   default_md = sha256
   distinguished_name = dn

   [ v3_ext ]
   authorityKeyIdentifier=keyid,issuer:always
   basicConstraints=CA:FALSE
   keyUsage=critical, digitalSignature, keyEncipherment
   extendedKeyUsage=serverAuth
   subjectAltName=@alt_names

   [ dn ]
   CN = broker

   [ alt_names ]
   DNS.1 = pulsar
   DNS.2 = pulsar.default
   IP.1 = 127.0.0.1
   IP.2 = 192.168.1.2
   ```
   
   :::tip

   To configure [hostname verification](#hostname-verification), you need to enter the hostname of the broker in `alt_names` as the Subject Alternative Name (SAN). To ensure that multiple machines can reuse the same certificate, you can also use a wildcard to match a group of broker hostnames, for example, `*.broker.usw.example.com`.

   :::

3. Generate the certificate request.

   ```bash
   openssl req -new -config broker.conf -key broker.key.pem -out broker.csr.pem -sha256
   ```

4. Sign the certificate with the CA.

   ```bash
   openssl x509 -req -in broker.csr.pem -CA ca.cert.pem -CAkey ca.key.pem -CAcreateserial -out broker.cert.pem -days 365 -extensions v3_ext -extfile broker.conf -sha256
   ```

At this point, you have a cert, `broker.cert.pem`, and a key, `broker.key-pk8.pem`, which you can use along with `ca.cert.pem` to configure TLS encryption for your brokers and proxies.

#### Create a client certificate

1. Generate the client's private key.

   ```bash
   openssl genrsa -out client.key.pem 2048
   ```

   The client expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format. Enter the following command to convert it.

   ```bash
   openssl pkcs8 -topk8 -inform PEM -outform PEM -in client.key.pem -out client.key-pk8.pem -nocrypt
   ```

2. Generate the certificate request. Note that the value of `CN` is used as the client's role token.

   ```bash
   openssl req -new -subj "/CN=client" -key client.key.pem -out client.csr.pem -sha256
   ```

3. Sign the certificate with the CA.

   ```bash
   openssl x509 -req -in client.csr.pem -CA ca.cert.pem -CAkey ca.key.pem -CAcreateserial -out client.cert.pem -days 365 -sha256
   ```

At this point, you have a cert `client.cert.pem` and a key `client.key-pk8.pem`, which you can use along with `ca.cert.pem` to configure TLS encryption for your clients.

### Configure brokers

To configure a Pulsar [broker](reference-terminology.md#broker) to use TLS encryption, you need to add these values to `broker.conf` in the `conf` directory of your Pulsar installation. Substitute the appropriate certificate paths where necessary.

```properties
brokerServicePortTls=6651
webServicePortTls=8081
tlsRequireTrustedClientCertOnConnect=true
tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem

brokerClientTlsEnabled=true
brokerClientTrustCertsFilePath=/path/to/ca.cert.pem
brokerClientCertificateFilePath=/path/to/client.cert.pem
brokerClientKeyFilePath=/path/to/client.key-pk8.pem
```

#### Configure TLS Protocol Version and Cipher

To configure the broker (and proxy) to require specific TLS protocol versions and ciphers for TLS negotiation, you can use the TLS protocol versions and ciphers to stop clients from requesting downgraded TLS protocol versions or ciphers that may have weaknesses.

By default, Pulsar uses OpenSSL when it is available, otherwise, Pulsar defaults back to the JDK implementation. OpenSSL currently supports `TLSv1.1`, `TLSv1.2` and `TLSv1.3`. You can acquire a list of supported ciphers from the OpenSSL ciphers command, i.e. `openssl ciphers -tls1_3`.

Both the TLS protocol versions and cipher properties can take multiple values, separated by commas. The possible values for protocol versions and ciphers depend on the TLS provider that you are using. 

```properties
tlsProtocols=TLSv1.3,TLSv1.2
tlsCiphers=TLS_DH_RSA_WITH_AES_256_GCM_SHA384,TLS_DH_RSA_WITH_AES_256_CBC_SHA
```

* `tlsProtocols=TLSv1.3,TLSv1.2`: List out the TLS protocols that you are going to accept from clients. By default, it is not set.
* `tlsCiphers=TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`: A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS network protocol. By default, it is null. See [OpenSSL Ciphers](https://www.openssl.org/docs/man1.0.2/apps/ciphers.html) and [JDK Ciphers](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites) for more details.

For JDK 11, you can obtain a list of supported values from the documentation:
- [TLS protocol](https://docs.oracle.com/en/java/javase/11/security/oracle-providers.html#GUID-7093246A-31A3-4304-AC5F-5FB6400405E2__SUNJSSEPROVIDERPROTOCOLPARAMETERS-BBF75009)
- [Ciphers](https://docs.oracle.com/en/java/javase/11/security/oracle-providers.html#GUID-7093246A-31A3-4304-AC5F-5FB6400405E2__SUNJSSE_CIPHER_SUITES)

### Configure proxies

Configuring TLS on proxies includes two directions of connections, from clients to proxies, and from proxies to brokers.

```properties
servicePortTls=6651
webServicePortTls=8081

# For clients connecting to the proxy
tlsRequireTrustedClientCertOnConnect=true
tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem

# For the proxy to connect to brokers
tlsEnabledWithBroker=true
brokerClientTrustCertsFilePath=/path/to/ca.cert.pem
brokerClientCertificateFilePath=/path/to/client.cert.pem
brokerClientKeyFilePath=/path/to/client.key-pk8.pem
```

### Configure clients

To enable TLS encryption, you need to configure the clients to use `https://` with port 8443 for the web service URL, and `pulsar+ssl://` with port 6651 for the broker service URL.

As the server certificate that you generated above does not belong to any of the default trust chains, you also need to either specify the path of the **trust cert** (recommended) or enable the clients to allow untrusted server certs.

The following examples show how to configure TLS encryption for Java/Python/C++/Node.js/C#/WebSocket clients.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"},{"label":"C#","value":"C#"},{"label":"WebSocket API","value":"WebSocket API"}]}>
<TabItem value="Java">

```java
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar+ssl://broker.example.com:6651/")
    .tlsKeyFilePath("/path/to/client.key-pk8.pem")
    .tlsCertificateFilePath("/path/to/client.cert.pem")
    .tlsTrustCertsFilePath("/path/to/ca.cert.pem")
    .enableTlsHostnameVerification(false) // false by default, in any case
    .allowTlsInsecureConnection(false) // false by default, in any case
    .build();
```

</TabItem>
<TabItem value="Python">

```python
from pulsar import Client

client = Client("pulsar+ssl://broker.example.com:6651/",
                tls_hostname_verification=False,
                tls_trust_certs_file_path="/path/to/ca.cert.pem",
                tls_allow_insecure_connection=False) // defaults to false from v2.2.0 onwards
```

</TabItem>
<TabItem value="C++">

```cpp
#include <pulsar/Client.h>

ClientConfiguration config = ClientConfiguration();
config.setUseTls(true);  // shouldn't be needed soon
config.setTlsTrustCertsFilePath(caPath);
config.setTlsAllowInsecureConnection(false);
config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));
config.setValidateHostName(false);
```

</TabItem>
<TabItem value="Node.js">

```javascript
const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar+ssl://broker.example.com:6651/',
    tlsTrustCertsFilePath: '/path/to/ca.cert.pem',
    useTls: true,
    tlsValidateHostname: false,
    tlsAllowInsecureConnection: false,
  });
})();
```

</TabItem>
<TabItem value="C#">

```csharp
var certificate = new X509Certificate2("ca.cert.pem");
var client = PulsarClient.Builder()
                         .TrustedCertificateAuthority(certificate) //If the CA is not trusted on the host, you can add it explicitly.
                         .VerifyCertificateAuthority(true) //Default is 'true'
                         .VerifyCertificateName(false)     //Default is 'false'
                         .Build();
```

> Note that `VerifyCertificateName` refers to the configuration of hostname verification in the C# client.

</TabItem>
<TabItem value="WebSocket API">

```python
import websockets
import asyncio
import base64
import json
import ssl
import pathlib

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
client_cert_pem = pathlib.Path(__file__).with_name("client.cert.pem")
client_key_pem = pathlib.Path(__file__).with_name("client.key.pem")
ca_cert_pem = pathlib.Path(__file__).with_name("ca.cert.pem")
ssl_context.load_cert_chain(certfile=client_cert_pem, keyfile=client_key_pem)
ssl_context.load_verify_locations(ca_cert_pem)
# websocket producer uri wss, not ws
uri = "wss://localhost:8080/ws/v2/producer/persistent/public/default/testtopic"
client_pem = pathlib.Path(__file__).with_name("pulsar_client.pem")
ssl_context.load_verify_locations(client_pem)
# websocket producer uri wss, not ws
uri = "wss://localhost:8080/ws/v2/producer/persistent/public/default/testtopic"
# encode message
s = "Hello World"
firstEncoded = s.encode("UTF-8")
binaryEncoded = base64.b64encode(firstEncoded)
payloadString = binaryEncoded.decode('UTF-8')
async def producer_handler(websocket):
    await websocket.send(json.dumps({
            'payload' : payloadString,
            'properties': {
                'key1' : 'value1',
                'key2' : 'value2'
            },
            'context' : 5
        }))
async def test():
    async with websockets.connect(uri) as websocket:
        await producer_handler(websocket)
        message = await websocket.recv()
        print(f"< {message}")
asyncio.run(test())
```

> Note that in addition to the required configurations in the `conf/client.conf` file, you need to configure more parameters in the `conf/broker.conf` file to enable TLS encryption on WebSocket service. For more details, see [security settings for WebSocket](client-libraries-websocket.md/#security-settings).

</TabItem>
</Tabs>
````

### Configure CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools.md), [`pulsar-perf`](reference-cli-tools.md), and [`pulsar-client`](reference-cli-tools.md) use the `conf/client.conf` config file in a Pulsar installation.

To use TLS encryption with Pulsar CLI tools, you need to add the following parameters to the `conf/client.conf` file.

```properties
webServiceUrl=https://broker.example.com:8443/
brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/ca.cert.pem
tlsKeyFilePath=/path/to/client.key-pk8.pem
tlsCertFile=/path/to/client-cert.pem
tlsEnableHostnameVerification=false
```

## Configure TLS encryption with KeyStore

By default, Pulsar uses [Conscrypt](https://github.com/google/conscrypt) for both broker service and Web service.

### Generate JKS certificate

You can use Java’s `keytool` utility to generate the key and certificate for each machine in the cluster. 

```bash
DAYS=365
CLIENT_COMMON_PARAMS="-storetype JKS -storepass clientpw -keypass clientpw -noprompt"
BROKER_COMMON_PARAMS="-storetype JKS -storepass brokerpw -keypass brokerpw -noprompt"

# create keystore
keytool -genkeypair -keystore broker.keystore.jks ${BROKER_COMMON_PARAMS} -keyalg RSA -keysize 2048 -alias broker -validity $DAYS \
-dname 'CN=broker,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown'
keytool -genkeypair -keystore client.keystore.jks ${CLIENT_COMMON_PARAMS} -keyalg RSA -keysize 2048 -alias client -validity $DAYS \
-dname 'CN=client,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown'

# export certificate 
keytool -exportcert -keystore broker.keystore.jks ${BROKER_COMMON_PARAMS} -file broker.cer -alias broker
keytool -exportcert -keystore client.keystore.jks ${CLIENT_COMMON_PARAMS} -file client.cer -alias client
 
# generate truststore
keytool -importcert -keystore client.truststore.jks ${CLIENT_COMMON_PARAMS} -file broker.cer -alias truststore
keytool -importcert -keystore broker.truststore.jks ${BROKER_COMMON_PARAMS} -file client.cer -alias truststore
```
 
:::note
 
To configure [hostname verification](#hostname-verification), you need to append ` -ext SAN=IP:127.0.0.1,IP:192.168.20.2,DNS:broker.example.com` to the value of `BROKER_COMMON_PARAMS` as the Subject Alternative Name (SAN).

:::


### Configure brokers

Configure the following parameters in the `conf/broker.conf` file and restrict access to the store files via filesystem permissions.

```properties
brokerServicePortTls=6651
webServicePortTls=8081

# Trusted client certificates are required to connect TLS
# Reject the Connection if the Client Certificate is not trusted.
# In effect, this requires that all connecting clients perform TLS client
# authentication.
tlsRequireTrustedClientCertOnConnect=true
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
brokerClientTlsKeyStoreType=JKS
brokerClientTlsKeyStore=/var/private/tls/client.keystore.jks
brokerClientTlsKeyStorePassword=clientpw
```

To disable non-TLS ports, you need to set the values of `brokerServicePort` and `webServicePort` to empty.

:::note

The default value of `tlsRequireTrustedClientCertOnConnect` is `false`. When it's enabled for mutual TLS, brokers/proxies require trusted client certificates; otherwise, brokers/proxies reject connection requests from clients.

:::

### Configure proxies

Configuring TLS on proxies includes two directions of connections, from clients to proxies, and from proxies to brokers.

```properties
servicePortTls=6651
webServicePortTls=8081
 
tlsRequireTrustedClientCertOnConnect=true
 
# keystore
tlsKeyStoreType=JKS
tlsKeyStore=/var/private/tls/proxy.keystore.jks
tlsKeyStorePassword=brokerpw
 
# truststore
tlsTrustStoreType=JKS
tlsTrustStore=/var/private/tls/proxy.truststore.jks
tlsTrustStorePassword=brokerpw
 
# internal client/admin-client config
tlsEnabledWithKeyStore=true
brokerClientTlsEnabled=true
brokerClientTlsEnabledWithKeyStore=true
brokerClientTlsTrustStoreType=JKS
brokerClientTlsTrustStore=/var/private/tls/client.truststore.jks
brokerClientTlsTrustStorePassword=clientpw
brokerClientTlsKeyStoreType=JKS
brokerClientTlsKeyStore=/var/private/tls/client.keystore.jks
brokerClientTlsKeyStorePassword=clientpw
```

### Configure clients

Similar to [Configure TLS encryption with PEM](security-tls-transport.md#configure-clients), you need to provide the TrustStore information for a minimal configuration.

The following is an example.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java client"
  values={[{"label":"Java client","value":"Java client"},{"label":"Java admin client","value":"Java admin client"}]}>
<TabItem value="Java client">

```java
    import org.apache.pulsar.client.api.PulsarClient;

    PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar+ssl://broker.example.com:6651/")
        .useKeyStoreTls(true)
        .tlsTrustStoreType("JKS")
        .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
        .tlsTrustStorePassword("clientpw")
        .tlsKeyStoreType("JKS")
        .tlsKeyStorePath("/var/private/tls/client.keystore.jks")
        .tlsKeyStorePassword("clientpw")
        .enableTlsHostnameVerification(false) // false by default, in any case
        .allowTlsInsecureConnection(false) // false by default, in any case
        .build();
```

</TabItem>
<TabItem value="Java admin client">

```java
    PulsarAdmin amdin = PulsarAdmin.builder().serviceHttpUrl("https://broker.example.com:8443")
        .tlsTrustStoreType("JKS")
        .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
        .tlsTrustStorePassword("clientpw")
        .tlsKeyStoreType("JKS")
        .tlsKeyStorePath("/var/private/tls/client.keystore.jks")
        .tlsKeyStorePassword("clientpw")
        .enableTlsHostnameVerification(false) // false by default, in any case
        .allowTlsInsecureConnection(false) // false by default, in any case
        .build();
```

</TabItem>
</Tabs>
````

### Configure CLI tools

For [Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools#pulsar-admin), [`pulsar-perf`](reference-cli-tools#pulsar-perf), and [`pulsar-client`](reference-cli-tools#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

   ```properties
   webServiceUrl=https://broker.example.com:8443/
   brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
   useKeyStoreTls=true
   tlsTrustStoreType=JKS
   tlsTrustStorePath=/var/private/tls/client.truststore.jks
   tlsTrustStorePassword=clientpw
   tlsKeyStoreType=JKS
   tlsKeyStorePath=/var/private/tls/client.keystore.jks
   keyStorePassword=clientpw
   ```

:::note

If you set `useKeyStoreTls` to `true`, be sure to configure `tlsTrustStorePath`.

:::

## Enable TLS Logging

You can enable TLS debug logging at the JVM level by starting the brokers and/or clients with `javax.net.debug` system property. For example:

```shell
-Djavax.net.debug=all
```

For more details, see [Oracle documentation](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html).
