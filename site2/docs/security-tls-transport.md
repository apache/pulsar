---
id: security-tls-transport
title: Transport Encryption using TLS
sidebar_label: "Transport Encryption using TLS"
---


````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

## TLS overview

By default, Apache Pulsar clients communicate with the Apache Pulsar service in plain text. This means that all data is sent in the clear. You can use TLS to encrypt this traffic to protect the traffic from the snooping of a man-in-the-middle attacker.

You can also configure TLS for both encryption and authentication. Use this guide to configure just TLS transport encryption and refer to [here](security-tls-authentication.md) for TLS authentication configuration. Alternatively, you can use [another authentication mechanism](security-athenz.md) on top of TLS transport encryption.

> Note that enabling TLS may impact the performance due to encryption overhead.

## TLS concepts

TLS is a form of [public key cryptography](https://en.wikipedia.org/wiki/Public-key_cryptography). Using key pairs consisting of a public key and a private key can perform the encryption. The public key encrypts the messages and the private key decrypts the messages.

To use TLS transport encryption, you need two kinds of key pairs, **server key pairs** and a **certificate authority**.

You can use a third kind of key pair, **client key pairs**, for [client authentication](security-tls-authentication.md).

You should store the **certificate authority** private key in a very secure location (a fully encrypted, disconnected, air gapped computer). As for the certificate authority public key, the **trust cert**, you can freely share it.

For both client and server key pairs, the administrator first generates a private key and a certificate request, then uses the certificate authority private key to sign the certificate request, and finally generates a certificate. This certificate is the public key for the server/client key pair.

For TLS transport encryption, the clients can use the **trust cert** to verify that the server has a key pair that the certificate authority signs when the clients are talking to the server. A man-in-the-middle attacker does not have access to the certificate authority, so they couldn't create a server with such a key pair.

For TLS authentication, the server uses the **trust cert** to verify that the client has a key pair that the certificate authority signed. The common name of the **client cert** is then used as the client's role token (see [Overview](security-overview.md)).

Pulsar uses [netty-tcnative](https://github.com/netty/netty-tcnative) and [Conscrypt](https://github.com/google/conscrypt) as security providers. There are two certificate formats:
* Java KeyStore(JKS): Pulsar uses Conscrypt by default for both broker service and Web service.
* CAcerts: Pulsar uses netty-tcnative by default, which includes two implementations, OpenSSL (default) and JDK. When OpenSSL is unavailable, JDK is used.

## Create TLS certificates

Creating TLS certificates for Pulsar involves creating a [certificate authority](#certificate-authority) (CA), [server certificate](#server-certificate), and [client certificate](#client-certificate).

Follow the guide below to set up a certificate authority. You can also refer to plenty of resources on the internet for more details. We recommend [this guide](https://jamielinux.com/docs/openssl-certificate-authority/index.html) for your detailed reference.

### Certificate authority

1. Create the certificate for the CA. You can use CA to sign both the broker and client certificates. This ensures that each party will trust the others. You should store CA in a very secure location (ideally completely disconnected from networks, air gapped, and fully encrypted).

2. Entering the following command to create a directory for your CA, and place [this openssl configuration file](https://github.com/apache/pulsar/tree/master/site2/website/static/examples/openssl.cnf) in the directory. You may want to modify the default answers for company name and department in the configuration file. Export the location of the CA directory to the environment variable, CA_HOME. The configuration file uses this environment variable to find the rest of the files and directories that the CA needs.

```bash
mkdir my-ca
cd my-ca
wget https://raw.githubusercontent.com/apache/pulsar-site/main/site2/website/static/examples/openssl.cnf
export CA_HOME=$(pwd)
```

3. Enter the commands below to create the necessary directories, keys and certs.

```bash
mkdir certs crl newcerts private
chmod 700 private/
touch index.txt
echo 1000 > serial
openssl genrsa -aes256 -out private/ca.key.pem 4096
# You need enter a password in the command above
chmod 400 private/ca.key.pem
openssl req -config openssl.cnf -key private/ca.key.pem \
    -new -x509 -days 7300 -sha256 -extensions v3_ca \
    -out certs/ca.cert.pem
# You must enter the same password in the previous openssl command
chmod 444 certs/ca.cert.pem
```

:::tip

The default `openssl` on macOS doesn't work for the commands above. You must upgrade the `openssl` via Homebrew:

```bash
brew install openssl
export PATH="/usr/local/Cellar/openssl@3/3.0.1/bin:$PATH"
```

The version `3.0.1` might change in the future. Use the actual path from the output of `brew install` command.

:::

4. After you answer the question prompts, CA-related files are stored in the `./my-ca` directory. Within that directory:

* `certs/ca.cert.pem` is the public certificate. This public certificate is meant to be distributed to all parties involved.
* `private/ca.key.pem` is the private key. You only need it when you are signing a new certificate for either broker or clients and you must safely guard this private key.

### Server certificate

Once you have created a CA certificate, you can create certificate requests and sign them with the CA.

The following commands ask you a few questions and then create the certificates. When you are asked for the common name, you should match the hostname of the broker. You can also use a wildcard to match a group of broker hostnames, for example, `*.broker.usw.example.com`. This ensures that multiple machines can reuse the same certificate.

:::tip

Sometimes matching the hostname is not possible or makes no sense,
such as when you create the brokers with random hostnames, or you
plan to connect to the hosts via their IP. In these cases, you
should configure the client to disable TLS hostname verification. For more
details, you can see [the host verification section in client configuration](#hostname-verification).

:::

1. Enter the command below to generate the key.

```bash
openssl genrsa -out broker.key.pem 2048
```

The broker expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so enter the following command to convert it.

```bash
openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in broker.key.pem -out broker.key-pk8.pem -nocrypt
```

2. Enter the following command to generate the certificate request.

```bash
openssl req -config openssl.cnf \
    -key broker.key.pem -new -sha256 -out broker.csr.pem
```

3. Sign it with the certificate authority by entering the command below.

```bash
openssl ca -config openssl.cnf -extensions server_cert \
    -days 1000 -notext -md sha256 \
    -in broker.csr.pem -out broker.cert.pem
```

At this point, you have a cert, `broker.cert.pem`, and a key, `broker.key-pk8.pem`, which you can use along with `ca.cert.pem` to configure TLS transport encryption for your broker and proxy nodes.

### Client certificate

1. Enter the command below to generate the key.

```bash
openssl genrsa -out client.key.pem 2048
```

The client expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so enter the following command to convert it.

```bash
openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in client.key.pem -out client.key-pk8.pem -nocrypt
```

2. Enter the following command to generate the certificate request.

```bash
openssl req -config openssl.cnf \
    -key client.key.pem -new -sha256 -out client.csr.pem
```

3. Sign it with the certificate authority by entering the command below.

```bash
openssl ca -config openssl.cnf -extensions client_cert \
    -days 1000 -notext -md sha256 \
    -in client.csr.pem -out client.cert.pem
```

At this point, you have a cert `client.cert.pem` and a key `client.key-pk8.pem`, which you can use along with `ca.cert.pem` to configure TLS encryption for your client.

## Configure brokers

To configure a Pulsar [broker](reference-terminology.md#broker) to use TLS transport encryption, you need to make some changes to `broker.conf`, which locates in the `conf` directory of your [Pulsar installation](getting-started-standalone.md).

Add these values to the configuration file (substituting the appropriate certificate paths where necessary):

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

> You can find a full list of parameters available in the `conf/broker.conf` file,
> as well as the default values for those parameters, in [Broker Configuration](reference-configuration.md#broker)

### TLS Protocol Version and Cipher

You can configure the broker (and proxy) to require specific TLS protocol versions and ciphers for TLS negiotation. You can use the TLS protocol versions and ciphers to stop clients from requesting downgraded TLS protocol versions or ciphers that may have weaknesses.

Both the TLS protocol versions and cipher properties can take multiple values, separated by commas. The possible values for protocol version and ciphers depend on the TLS provider that you are using. Pulsar uses OpenSSL if the OpenSSL is available, but if the OpenSSL is not available, Pulsar defaults back to the JDK implementation.

```properties
tlsProtocols=TLSv1.3,TLSv1.2
tlsCiphers=TLS_DH_RSA_WITH_AES_256_GCM_SHA384,TLS_DH_RSA_WITH_AES_256_CBC_SHA
```

OpenSSL currently supports `TLSv1.1`, `TLSv1.2` and `TLSv1.3` for the protocol version. You can acquire a list of supported cipher from the openssl ciphers command, i.e. `openssl ciphers -tls1_3`.

For JDK 11, you can obtain a list of supported values from the documentation:
- [TLS protocol](https://docs.oracle.com/en/java/javase/11/security/oracle-providers.html#GUID-7093246A-31A3-4304-AC5F-5FB6400405E2__SUNJSSEPROVIDERPROTOCOLPARAMETERS-BBF75009)
- [Ciphers](https://docs.oracle.com/en/java/javase/11/security/oracle-providers.html#GUID-7093246A-31A3-4304-AC5F-5FB6400405E2__SUNJSSE_CIPHER_SUITES)

## Configure proxies

Proxies need to configure TLS in two directions, for clients connecting to the proxy, and for the proxy connecting to brokers.

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

## Configure clients

When you enable the TLS transport encryption, you need to configure the client to use `https://` and port 8443 for the web service URL, and `pulsar+ssl://` and port 6651 for the broker service URL.

As the server certificate that you generated above does not belong to any of the default trust chains, you also need to either specify the path of the **trust cert** (recommended), or tell the client to allow untrusted server certs.

### Hostname verification

Hostname verification is a TLS security feature whereby a client can refuse to connect to a server if the "CommonName" does not match the hostname to which the hostname is connecting. By default, Pulsar clients disable hostname verification, as it requires that each broker has a DNS record and a unique cert.

Moreover, as the administrator has full control of the certificate authority, a bad actor is unlikely to be able to pull off a man-in-the-middle attack. "allowInsecureConnection" allows the client to connect to servers whose cert has not been signed by an approved CA. The client disables "allowInsecureConnection" by default, and you should always disable "allowInsecureConnection" in production environments. As long as you disable "allowInsecureConnection", a man-in-the-middle attack requires that the attacker has access to the CA.

One scenario where you may want to enable hostname verification is where you have multiple proxy nodes behind a VIP, and the VIP has a DNS record, for example, pulsar.mycompany.com. In this case, you can generate a TLS cert with pulsar.mycompany.com as the "CommonName," and then enable hostname verification on the client.

The examples below show that hostname verification is disabled for Java/Python/C++/Node.js/C# clients by default.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"},{"label":"C#","value":"C#"}]}>
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
</Tabs>
````

## Configure TLS encryption in CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools.md#pulsar-admin), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

To use TLS encryption with the CLI tools of Pulsar, you need to add the following parameters to the `conf/client.conf` file.

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

Apache Pulsar supports [TLS encryption](security-tls-transport.md) and [TLS authentication](security-tls-authentication.md) between clients and Apache Pulsar service. By default, it uses PEM format file configuration. This section tries to describe how to use [KeyStore](https://en.wikipedia.org/wiki/Java_KeyStore) type to configure TLS.

### Generate TLS key and certificate

The first step of deploying TLS is to generate the key and the certificate for each machine in the cluster. You can use Java’s `keytool` utility to accomplish this task. We will generate the key into a temporary keystore initially for broker, so that we can export and sign it later with CA.

```shell
keytool -keystore broker.keystore.jks -alias localhost -validity {validity} -genkeypair -keyalg RSA
```

You need to specify two parameters in the above command:

1. `keystore`: the keystore file that stores the certificate. The *keystore* file contains the private key of
   the certificate; hence, it needs to be kept safely.
2. `validity`: the valid time of the certificate in days.

> Ensure that common name (CN) matches exactly with the fully qualified domain name (FQDN) of the server.
The client compares the CN with the DNS domain name to ensure that it is indeed connecting to the desired server, not a malicious one.

### Create your own CA

After the first step, each broker in the cluster has a public-private key pair, and a certificate to identify the machine.
The certificate, however, is unsigned, which means that an attacker can create such a certificate to pretend to be any machine.

Therefore, it is important to prevent forged certificates by signing them for each machine in the cluster.
A `certificate authority (CA)` is responsible for signing certificates. CA works like a government that issues passports —
the government stamps (signs) each passport so that the passport becomes difficult to forge. Other governments verify the stamps
to ensure the passport is authentic. Similarly, the CA signs the certificates, and the cryptography guarantees that a signed
certificate is computationally difficult to forge. Thus, as long as the CA is a genuine and trusted authority, the clients have
high assurance that they are connecting to authentic machines.

```shell
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365
```

The generated CA is simply a *public-private* key pair and certificate, and it is intended to sign other certificates.

The next step is to add the generated CA to the clients' truststore so that the clients can trust this CA:

```shell
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
```

:::note

If you configure the brokers to require client authentication by setting `tlsRequireTrustedClientCertOnConnect` to `true` on the broker configuration, then you must also provide a truststore for the brokers and it should have all the CA certificates that clients keys were signed by.

:::

```shell
keytool -keystore broker.truststore.jks -alias CARoot -import -file ca-cert
```

In contrast to the keystore, which stores each machine’s own identity, the truststore of a client stores all the certificates
that the client should trust. Importing a certificate into one’s truststore also means trusting all certificates that are signed
by that certificate. As the analogy above, trusting the government (CA) also means trusting all passports (certificates) that
it has issued. This attribute is called the chain of trust, and it is particularly useful when deploying TLS on a large BookKeeper cluster.
You can sign all certificates in the cluster with a single CA, and have all machines share the same truststore that trusts the CA.
That way all machines can authenticate all other machines.


### Sign the certificate

The next step is to sign all certificates in the keystore with the CA we generated. 

1. Export the certificate from the keystore:

```shell
keytool -keystore broker.keystore.jks -alias localhost -certreq -file cert-file
keytool -keystore client.keystore.jks -alias localhost -certreq -file cert-file
```

2. Sign it with the CA:

```shell
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days {validity} -CAcreateserial -passin pass:{ca-password}
```

3. Import both the certificate of the CA and the signed certificate into the keystore:

```shell
keytool -keystore broker.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore broker.keystore.jks -alias localhost -import -file cert-signed

keytool -keystore client.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore client.keystore.jks -alias localhost -import -file cert-signed
```

The definitions of the parameters are the following:

1. `keystore`: the location of the keystore
2. `ca-cert`: the certificate of the CA
3. `ca-key`: the private key of the CA
4. `ca-password`: the passphrase of the CA
5. `cert-file`: the exported, unsigned certificate of the broker
6. `cert-signed`: the signed certificate of the broker

### Configure brokers

Brokers enable TLS by providing valid `brokerServicePortTls` and `webServicePortTls`, and also set `tlsEnabledWithKeyStore` to `true` for using KeyStore type configuration. Besides this, KeyStore path,  KeyStore password, TrustStore path, and TrustStore password need to be provided. And since brokers create internal client/admin client to communicate with other brokers, users also need to provide config for them, this is similar to how users configure the outside client/admin-client. If `tlsRequireTrustedClientCertOnConnect` is `true`, brokers reject the Connection if the Client Certificate is not trusted. 

The following TLS configs are needed on the broker side:

```properties
brokerServicePortTls=6651
webServicePortTls=8081

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

:::note

It is important to restrict access to the store files via filesystem permissions.

:::

If you have configured TLS on the broker, to disable non-TLS ports, you can set the values of the following configurations to empty as below.

```conf
brokerServicePort=
webServicePort=
```

In this case, you need to set the following configurations.

```properties
brokerClientTlsEnabled=true // Set this to true
brokerClientTlsEnabledWithKeyStore=true  // Set this to true
brokerClientTlsTrustStore= // Set this to your desired value
brokerClientTlsTrustStorePassword= // Set this to your desired value
```

Optional settings that may worth considering:

1. tlsClientAuthentication=false: Enable/Disable using TLS for authentication. This config when enabled will authenticate the other end
   of the communication channel. It should be enabled on both brokers and clients for mutual TLS.
2. tlsCiphers=[TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256], A cipher suite is a named combination of authentication, encryption, MAC and key exchange
   algorithm used to negotiate the security settings for a network connection using TLS network protocol. By default,
   it is null. [OpenSSL Ciphers](https://www.openssl.org/docs/man1.0.2/apps/ciphers.html)
   [JDK Ciphers](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites)
3. tlsProtocols=[TLSv1.3,TLSv1.2] (list out the TLS protocols that you are going to accept from clients).
   By default, it is not set.

### Configure Clients

This is similar to [TLS encryption configurations for clients with PEM type](security-tls-transport.md#configure-clients).
For a minimal configuration, you need to provide the TrustStore information.

For example:
1. for [Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools#pulsar-admin), [`pulsar-perf`](reference-cli-tools#pulsar-perf), and [`pulsar-client`](reference-cli-tools#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

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

2. for Java client

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

3. for Java admin client

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

:::note

Configure `tlsTrustStorePath` when you set `useKeyStoreTls` to `true`.

:::

## Enable TLS Logging

You can enable TLS debug logging at the JVM level by starting the brokers and/or clients with `javax.net.debug` system property. For example:

```shell
-Djavax.net.debug=all
```

You can find more details on this in [Oracle documentation](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html) on [debugging SSL/TLS connections](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html).
