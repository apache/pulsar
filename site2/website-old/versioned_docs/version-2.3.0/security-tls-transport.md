---
id: version-2.3.0-security-tls-transport
title: Transport Encryption using TLS
sidebar_label: Transport Encryption using TLS
original_id: security-tls-transport
---

## TLS Overview

By default, Apache Pulsar clients communicate with the Apache Pulsar service in plain text, which means that all data is sent in the clear. TLS can be used to encrypt this traffic so that it cannot be snooped by a man-in-the-middle attacker.

TLS can be configured for both encryption and authentication. You may configure just TLS transport encryption, which is covered in this guide. TLS authentication is covered [elsewhere](security-tls-authentication.md). Alternatively, you can use [another authentication mechanism](security-athenz.md) on top of TLS transport encryption.

> Note that enabling TLS may have a performance impact due to encryption overhead.

## TLS concepts

TLS is a form of [public key cryptography](https://en.wikipedia.org/wiki/Public-key_cryptography). Encryption is performed using key pairs consisting of a public key and a private key. Messages are encrypted with the public key and can be decrypted with the private key.

To use TLS transport encryption, you need two kinds of key pairs, **server key pairs** and a **certificate authority**.

A third kind of key pair, **client key pairs**, are used for [client authentication](security-tls-authentication.md).

The **certificate authority** private key should be stored in a very secure location (a fully encrypted, disconnected, air gapped computer). The certificate authority public key, the **trust cert**, can be freely shared.

For both client and server key pairs, the administrator first generates a private key and a certificate request. Then the certificate authority private key is used to sign the certificate request, generating a certificate. This certificate is the public key for the server/client key pair.

For TLS transport encryption, the clients can use the **trust cert** to verify that the server they are talking to has a key pair that was signed by the certificate authority. A man-in-the-middle attacker would not have access to the certificate authority, so they couldn't create a server with such a key pair.

For TLS authentication, the server uses the **trust cert** to verify that the client has a key pair that was signed by the certificate authority. The Common Name of the **client cert** is then used as the client's role token (see [Overview](security-overview.md)).

## Creating TLS Certificates

Creating TLS certificates for Pulsar involves creating a [certificate authority](#certificate-authority) (CA), [server certificate](#server-certificate), and [client certificate](#client-certificate).

The following guide is an abridged guide to setting up a certificate authority. For a more detailed guide, there are plenty of resource on the internet. We recommend the [this guide](https://jamielinux.com/docs/openssl-certificate-authority/index.html).

### Certificate authority

The first step is to create the certificate for the CA. The CA will be used to sign both the broker and client certificates, in order to ensure that each party will trust the others. The CA should be stored in a very secure location (ideally completely disconnected from networks, air gapped, and fully encrypted).

Create a directory for your CA, and place [this openssl configuration file](https://github.com/apache/pulsar/tree/master/site2/website/static/examples/openssl.cnf) in the directory. You may want to modify the default answers for company name and department in the configuration file. Export the location of the CA directory to the environment variable, CA_HOME. The configuration file uses this environment variable to find the rest of the files and directories needed for the CA.

```bash
mkdir my-ca
cd my-ca
wget https://raw.githubusercontent.com/apache/pulsar/master/site2/website/static/examples/openssl.cnf
export CA_HOME=$(pwd)
```

Create the necessary directories, keys and certs.

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

After answering the question prompts, this will store CA-related files in the `./my-ca` directory. Within that directory:

* `certs/ca.cert.pem` is the public certificate. It is meant to be distributed to all parties involved.
* `private/ca.key.pem` is the private key. This is only needed when signing a new certificate for either broker or clients and it must be safely guarded.

### Server certificate

Once a CA certificate has been created, you can create certificate requests and sign them with the CA.

The following commands will ask you a few questions and then create the certificates. When asked for the common name, you should match the hostname of the broker. You could also use a wildcard to match a group of broker hostnames, for example `*.broker.usw.example.com`. This ensures that the same certificate can be reused on multiple machines.

> #### Tips
> 
> Sometimes it is not possible or makes no sense to match the hostname,
> such as when the brokers are created with random hostnames, or you
> plan to connect to the hosts via their IP. In this case, the client
> should be configured to disable TLS hostname verification. For more
> details, see [the host verification section in client configuration](#hostname-verification).

First generate the key.
```bash
openssl genrsa -out broker.key.pem 2048
```

The broker expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so convert it.

```bash
openssl pkcs8 -topk8 -inform PEM -outform PEM \
    -in broker.key.pem -out broker.key-pk8.pem -nocrypt
```

Generate the certificate request...

```bash
openssl req -config openssl.cnf \
    -key broker.key.pem -new -sha256 -out broker.csr.pem
```

... and sign it with the certificate authority.
```bash
openssl ca -config openssl.cnf -extensions server_cert \
    -days 1000 -notext -md sha256 \
    -in broker.csr.pem -out broker.cert.pem
```

At this point, you have a cert, `broker.cert.pem`, and a key, `broker.key-pk8.pem`, which can be used along with `ca.cert.pem` to configure TLS transport encryption for your broker and proxy nodes.

## Broker Configuration

To configure a Pulsar [broker](reference-terminology.md#broker) to use TLS transport encryption, you'll need to make some changes to `broker.conf`, which is located in the `conf` directory of your [Pulsar installation](getting-started-standalone.md).

Add these values to the configuration file (substituting the appropriate certificate paths where necessary):

```properties
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem
```

> A full list of parameters available in the `conf/broker.conf` file,
> as well as the default values for those parameters, can be found in [Broker Configuration](reference-configuration.md#broker) 

### TLS Protocol Version and Cipher

The broker (and proxy) can be configured to require specific TLS protocol versions and ciphers for TLS negiotation. This can be used to stop clients from requesting downgraded TLS protocol versions or ciphers which may have weaknesses.

Both the TLS protocol versions and cipher properties can take multiple values, separated by commas. The possible values for protocol version and ciphers depend on the TLS provider being used. Pulsar uses OpenSSL if available, but if not defaults back to the JDK implementation.

```properties
tlsProtocols=TLSv1.2,TLSv1.1
tlsCiphers=TLS_DH_RSA_WITH_AES_256_GCM_SHA384,TLS_DH_RSA_WITH_AES_256_CBC_SHA
```

OpenSSL currently supports ```SSL2```, ```SSL3```, ```TLSv1```, ```TLSv1.1``` and ```TLSv1.2``` for the protocol version. A list of supported cipher can be acquired from the openssl ciphers command, i.e. ```openssl ciphers -tls_v2```.

For JDK 8, a list of supported values can be obtained from the documentation:
- [TLS protocol](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext)
- [Ciphers](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites)

## Proxy Configuration

Proxies need to configure TLS in two directions, for clients connecting to the proxy, and for the proxy to be able to connect to brokers.

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

When TLS transport encryption is enabled, you need to configure the client to use ```https://``` and port 8443 for the web service URL, and ```pulsar+ssl://``` and port 6651 for the broker service URL.

As the server certificate you generated above doesn't belong to any of the default trust chains, you also need to either specify the path the **trust cert** (recommended), or tell the client to allow untrusted server certs.

### Hostname verification

Hostname verification is a TLS security feature whereby a client can refuse to connect to a server if the "CommonName" does not match the hostname to which it is connecting. By default, Pulsar clients disable hostname verification, as it requires that each broker has a DNS record and a unique cert.

Moreover, as the administrator has full control of the certificate authority, it is unlikely that a bad actor would be able to pull off a man-in-the-middle attack. "allowInsecureConnection" allows the client to connect to servers whose cert has not been signed by an approved CA. The client disables it by default, and should always be disabled in production environments. As long as "allowInsecureConnection" is disabled, a man-in-the-middle attack would require that the attacker has access to the CA.

One scenario where you may want to enable hostname verification is where you have multiple proxy nodes behind a VIP, and the VIP has a DNS record, for example, pulsar.mycompany.com. In this case, you can generate a TLS cert with pulsar.mycompany.com as the "CommonName," and then enable hostname verification on the client.

The examples below show hostname verification being disabled for the Java client, though you can be omit this as the client disables it by default. C++/python clients do now allow this to be configured at the moment.

### CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools.md#pulsar-admin), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You'll need to add the following parameters to that file to use TLS transport with Pulsar's CLI tools:

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
