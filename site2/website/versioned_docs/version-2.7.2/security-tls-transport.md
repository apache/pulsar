---
id: version-2.7.2-security-tls-transport
title: Transport Encryption using TLS
sidebar_label: Transport Encryption using TLS
original_id: security-tls-transport
---

## TLS overview

By default, Apache Pulsar clients communicate with the Apache Pulsar service in plain text. This means that all data is sent in the clear. You can use TLS to encrypt this traffic to protect the traffic from the snooping of a man-in-the-middle attacker.

You can also configure TLS for both encryption and authentication. Use this guide to configure just TLS transport encryption and refer to [here](security-tls-authentication.md) for TLS authentication configuration. Alternatively, you can use [another authentication mechanism](security-athenz.md) on top of TLS transport encryption.

> Note that enabling TLS may impact the performance due to encryption overhead.

## TLS concepts

TLS is a form of [public key cryptography](https://en.wikipedia.org/wiki/Public-key_cryptography). Using key pairs consisting of a public key and a private key can perform the encryption. The public key encrpyts the messages and the private key decrypts the messages.

To use TLS transport encryption, you need two kinds of key pairs, **server key pairs** and a **certificate authority**.

You can use a third kind of key pair, **client key pairs**, for [client authentication](security-tls-authentication.md).

You should store the **certificate authority** private key in a very secure location (a fully encrypted, disconnected, air gapped computer). As for the certificate authority public key, the **trust cert**, you can freely shared it.

For both client and server key pairs, the administrator first generates a private key and a certificate request, then uses the certificate authority private key to sign the certificate request, finally generates a certificate. This certificate is the public key for the server/client key pair.

For TLS transport encryption, the clients can use the **trust cert** to verify that the server has a key pair that the certificate authority signed when the clients are talking to the server. A man-in-the-middle attacker does not have access to the certificate authority, so they couldn't create a server with such a key pair.

For TLS authentication, the server uses the **trust cert** to verify that the client has a key pair that the certificate authority signed. The common name of the **client cert** is then used as the client's role token (see [Overview](security-overview.md)).

`Bouncy Castle Provider` provides cipher suites and algorithms in Pulsar. If you need [FIPS](https://www.bouncycastle.org/fips_faq.html) version of `Bouncy Castle Provider`, please reference [Bouncy Castle page](security-bouncy-castle.md).

## Create TLS certificates

Creating TLS certificates for Pulsar involves creating a [certificate authority](#certificate-authority) (CA), [server certificate](#server-certificate), and [client certificate](#client-certificate).

Follow the guide below to set up a certificate authority. You can also refer to plenty of resources on the internet for more details. We recommend [this guide](https://jamielinux.com/docs/openssl-certificate-authority/index.html) for your detailed reference.

### Certificate authority

1. Create the certificate for the CA. You can use CA to sign both the broker and client certificates. This ensures that each party will trust the others. You should store CA in a very secure location (ideally completely disconnected from networks, air gapped, and fully encrypted).

2. Entering the following command to create a directory for your CA, and place [this openssl configuration file](https://github.com/apache/pulsar/tree/master/site2/website/static/examples/openssl.cnf) in the directory. You may want to modify the default answers for company name and department in the configuration file. Export the location of the CA directory to the environment variable, CA_HOME. The configuration file uses this environment variable to find the rest of the files and directories that the CA needs.

```bash
mkdir my-ca
cd my-ca
wget https://raw.githubusercontent.com/apache/pulsar/master/site2/website/static/examples/openssl.cnf
export CA_HOME=$(pwd)
```

3. Enter the commands below to create the necessary directories, keys and certs.

```bash
mkdir certs crl newcerts private
chmod 700 private/
touch index.txt
echo 1000 > serial
openssl genrsa -aes256 -out private/ca.key.pem 4096
chmod 400 private/ca.key.pem
openssl req -config openssl.cnf -key private/ca.key.pem \
    -new -x509 -days 7300 -sha256 -extensions v3_ca \
    -out certs/ca.cert.pem
chmod 444 certs/ca.cert.pem
```

4. After you answer the question prompts, CA-related files are stored in the `./my-ca` directory. Within that directory:

* `certs/ca.cert.pem` is the public certificate. This public certificates is meant to be distributed to all parties involved.
* `private/ca.key.pem` is the private key. You only need it when you are signing a new certificate for either broker or clients and you must safely guard this private key.

### Server certificate

Once you have created a CA certificate, you can create certificate requests and sign them with the CA.

The following commands ask you a few questions and then create the certificates. When you are asked for the common name, you should match the hostname of the broker. You can also use a wildcard to match a group of broker hostnames, for example, `*.broker.usw.example.com`. This ensures that multiple machines can reuse the same certificate.

> #### Tips
> 
> Sometimes matching the hostname is not possible or makes no sense,
> such as when you create the brokers with random hostnames, or you
> plan to connect to the hosts via their IP. In these cases, you 
> should configure the client to disable TLS hostname verification. For more
> details, you can see [the host verification section in client configuration](#hostname-verification).

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

## Broker Configuration

To configure a Pulsar [broker](reference-terminology.md#broker) to use TLS transport encryption, you need to make some changes to `broker.conf`, which locates in the `conf` directory of your [Pulsar installation](getting-started-standalone.md).

Add these values to the configuration file (substituting the appropriate certificate paths where necessary):

```properties
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem
```

> You can find a full list of parameters available in the `conf/broker.conf` file,
> as well as the default values for those parameters, in [Broker Configuration](reference-configuration.md#broker) 
> 
### TLS Protocol Version and Cipher

You can configure the broker (and proxy) to require specific TLS protocol versions and ciphers for TLS negiotation. You can use the TLS protocol versions and ciphers to stop clients from requesting downgraded TLS protocol versions or ciphers that may have weaknesses.

Both the TLS protocol versions and cipher properties can take multiple values, separated by commas. The possible values for protocol version and ciphers depend on the TLS provider that you are using. Pulsar uses OpenSSL if the OpenSSL is available, but if the OpenSSL is not available, Pulsar defaults back to the JDK implementation.

```properties
tlsProtocols=TLSv1.2,TLSv1.1
tlsCiphers=TLS_DH_RSA_WITH_AES_256_GCM_SHA384,TLS_DH_RSA_WITH_AES_256_CBC_SHA
```

OpenSSL currently supports ```SSL2```, ```SSL3```, ```TLSv1```, ```TLSv1.1``` and ```TLSv1.2``` for the protocol version. You can acquire a list of supported cipher from the openssl ciphers command, i.e. ```openssl ciphers -tls_v2```.

For JDK 8, you can obtain a list of supported values from the documentation:
- [TLS protocol](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext)
- [Ciphers](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites)

## Proxy Configuration

Proxies need to configure TLS in two directions, for clients connecting to the proxy, and for the proxy connecting to brokers.

```properties
# For clients connecting to the proxy
tlsEnabledInProxy=true
tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem

# For the proxy to connect to brokers
tlsEnabledWithBroker=true
brokerClientTrustCertsFilePath=/path/to/ca.cert.pem
```

## Client configuration

When you enable the TLS transport encryption, you need to configure the client to use ```https://``` and port 8443 for the web service URL, and ```pulsar+ssl://``` and port 6651 for the broker service URL.

As the server certificate that you generated above does not belong to any of the default trust chains, you also need to either specify the path the **trust cert** (recommended), or tell the client to allow untrusted server certs.

### Hostname verification

Hostname verification is a TLS security feature whereby a client can refuse to connect to a server if the "CommonName" does not match the hostname to which the hostname is connecting. By default, Pulsar clients disable hostname verification, as it requires that each broker has a DNS record and a unique cert.

Moreover, as the administrator has full control of the certificate authority, a bad actor is unlikely to be able to pull off a man-in-the-middle attack. "allowInsecureConnection" allows the client to connect to servers whose cert has not been signed by an approved CA. The client disables "allowInsecureConnection" by default, and you should always disable "allowInsecureConnection" in production environments. As long as you disable "allowInsecureConnection", a man-in-the-middle attack requires that the attacker has access to the CA.

One scenario where you may want to enable hostname verification is where you have multiple proxy nodes behind a VIP, and the VIP has a DNS record, for example, pulsar.mycompany.com. In this case, you can generate a TLS cert with pulsar.mycompany.com as the "CommonName," and then enable hostname verification on the client.

The examples below show hostname verification being disabled for the Java client, though you can omit this as the client disables the hostname verification by default. C++/python/Node.js clients do now allow configuring this at the moment.

### CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools.md#pulsar-admin), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You need to add the following parameters to that file to use TLS transport with the CLI tools of Pulsar:

```properties
webServiceUrl=https://broker.example.com:8443/
brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/ca.cert.pem
tlsEnableHostnameVerification=false
```

#### Java client

```java
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar+ssl://broker.example.com:6651/")
    .enableTls(true)
    .tlsTrustCertsFilePath("/path/to/ca.cert.pem")
    .enableTlsHostnameVerification(false) // false by default, in any case
    .allowTlsInsecureConnection(false) // false by default, in any case
    .build();
```

#### Python client

```python
from pulsar import Client

client = Client("pulsar+ssl://broker.example.com:6651/",
                tls_hostname_verification=True,
                tls_trust_certs_file_path="/path/to/ca.cert.pem",
                tls_allow_insecure_connection=False) // defaults to false from v2.2.0 onwards
```

#### C++ client

```c++
#include <pulsar/Client.h>

ClientConfiguration config = ClientConfiguration();
config.setUseTls(true);  // shouldn't be needed soon
config.setTlsTrustCertsFilePath(caPath);
config.setTlsAllowInsecureConnection(false);
config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));
config.setValidateHostName(true);
```

#### Node.js client

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar+ssl://broker.example.com:6651/',
    tlsTrustCertsFilePath: '/path/to/ca.cert.pem',
  });
})();
```

#### C# client

```c#
var certificate = new X509Certificate2("ca.cert.pem");
var client = PulsarClient.Builder()
                         .TrustedCertificateAuthority(certificate) //If the CA is not trusted on the host, you can add it explicitly.
                         .VerifyCertificateAuthority(true) //Default is 'true'
                         .VerifyCertificateName(false)     //Default is 'false'
                         .Build();
```
