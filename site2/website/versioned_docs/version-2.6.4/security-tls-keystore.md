---
id: version-2.6.4-security-tls-keystore
title: Using TLS with KeyStore configure
sidebar_label: Using TLS with KeyStore configure
original_id: security-tls-keystore
---

## Overview

Apache Pulsar supports [TLS encryption](security-tls-transport.md) and [TLS authentication](security-tls-authentication.md) between clients and Apache Pulsar service. 
By default it uses PEM format file configuration. This page tries to describe use [KeyStore](https://en.wikipedia.org/wiki/Java_KeyStore) type configure for TLS.


## TLS encryption with KeyStore configure
 
### Generate TLS key and certificate

The first step of deploying TLS is to generate the key and the certificate for each machine in the cluster.
You can use Java’s `keytool` utility to accomplish this task. We will generate the key into a temporary keystore
initially for broker, so that we can export and sign it later with CA.

```shell
keytool -keystore broker.keystore.jks -alias localhost -validity {validity} -genkey
```

You need to specify two parameters in the above command:

1. `keystore`: the keystore file that stores the certificate. The *keystore* file contains the private key of
    the certificate; hence, it needs to be kept safely.
2. `validity`: the valid time of the certificate in days.

> Ensure that common name (CN) matches exactly with the fully qualified domain name (FQDN) of the server.
The client compares the CN with the DNS domain name to ensure that it is indeed connecting to the desired server, not a malicious one.

### Creating your own CA

After the first step, each broker in the cluster has a public-private key pair, and a certificate to identify the machine.
The certificate, however, is unsigned, which means that an attacker can create such a certificate to pretend to be any machine.

Therefore, it is important to prevent forged certificates by signing them for each machine in the cluster.
A `certificate authority (CA)` is responsible for signing certificates. CA works likes a government that issues passports —
the government stamps (signs) each passport so that the passport becomes difficult to forge. Other governments verify the stamps
to ensure the passport is authentic. Similarly, the CA signs the certificates, and the cryptography guarantees that a signed
certificate is computationally difficult to forge. Thus, as long as the CA is a genuine and trusted authority, the clients have
high assurance that they are connecting to the authentic machines.

```shell
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365
```

The generated CA is simply a *public-private* key pair and certificate, and it is intended to sign other certificates.

The next step is to add the generated CA to the clients' truststore so that the clients can trust this CA:

```shell
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
```

NOTE: If you configure the brokers to require client authentication by setting `tlsRequireTrustedClientCertOnConnect` to `true` on the
broker configuration, then you must also provide a truststore for the brokers and it should have all the CA certificates that clients keys were signed by.

```shell
keytool -keystore broker.truststore.jks -alias CARoot -import -file ca-cert
```

In contrast to the keystore, which stores each machine’s own identity, the truststore of a client stores all the certificates
that the client should trust. Importing a certificate into one’s truststore also means trusting all certificates that are signed
by that certificate. As the analogy above, trusting the government (CA) also means trusting all passports (certificates) that
it has issued. This attribute is called the chain of trust, and it is particularly useful when deploying TLS on a large BookKeeper cluster.
You can sign all certificates in the cluster with a single CA, and have all machines share the same truststore that trusts the CA.
That way all machines can authenticate all other machines.


### Signing the certificate

The next step is to sign all certificates in the keystore with the CA we generated. First, you need to export the certificate from the keystore:

```shell
keytool -keystore broker.keystore.jks -alias localhost -certreq -file cert-file
```

Then sign it with the CA:

```shell
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days {validity} -CAcreateserial -passin pass:{ca-password}
```

Finally, you need to import both the certificate of the CA and the signed certificate into the keystore:

```shell
keytool -keystore broker.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore broker.keystore.jks -alias localhost -import -file cert-signed
```

The definitions of the parameters are the following:

1. `keystore`: the location of the keystore
2. `ca-cert`: the certificate of the CA
3. `ca-key`: the private key of the CA
4. `ca-password`: the passphrase of the CA
5. `cert-file`: the exported, unsigned certificate of the broker
6. `cert-signed`: the signed certificate of the broker

### Configuring brokers

Brokers enable TLS by provide valid `brokerServicePortTls` and `webServicePortTls`, and also need set `tlsEnabledWithKeyStore` to `true` for using KeyStore type configuration.
Besides this, KeyStore path,  KeyStore password, TrustStore path, and TrustStore password need to provided.
And since broker will create internal client/admin client to communicate with other brokers, user also need to provide config for them, this is similar to how user config the outside client/admin-client.
If `tlsRequireTrustedClientCertOnConnect` is `true`, broker will reject the Connection if the Client Certificate is not trusted. 

The following TLS configs are needed on the broker side:

```properties
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
```

NOTE: it is important to restrict access to the store files via filesystem permissions.

Optional settings that may worth consider:

1. tlsClientAuthentication=false: Enable/Disable using TLS for authentication. This config when enabled will authenticate the other end
    of the communication channel. It should be enabled on both brokers and clients for mutual TLS.
2. tlsCiphers=[TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256], A cipher suite is a named combination of authentication, encryption, MAC and key exchange
    algorithm used to negotiate the security settings for a network connection using TLS network protocol. By default,
    it is null. [OpenSSL Ciphers](https://www.openssl.org/docs/man1.0.2/apps/ciphers.html)
    [JDK Ciphers](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites)
3. tlsProtocols=[TLSv1.2,TLSv1.1,TLSv1] (list out the TLS protocols that you are going to accept from clients).
    By default, it is not set.

### Configuring Clients

This is similar to [TLS encryption configuing for client with PEM type](security-tls-transport.md#Client configuration).
For a a minimal configuration, user need to provide the TrustStore information.

e.g. 
1. for [Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools#pulsar-admin), [`pulsar-perf`](reference-cli-tools#pulsar-perf), and [`pulsar-client`](reference-cli-tools#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

    ```properties
    webServiceUrl=https://broker.example.com:8443/
    brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
    useKeyStoreTls=true
    tlsTrustStoreType=JKS
    tlsTrustStorePath=/var/private/tls/client.truststore.jks
    tlsTrustStorePassword=clientpw
    ```

1. for java client
    ```java
    import org.apache.pulsar.client.api.PulsarClient;
    
    PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar+ssl://broker.example.com:6651/")
        .enableTls(true)
        .useKeyStoreTls(true)
        .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
        .tlsTrustStorePassword("clientpw")
        .allowTlsInsecureConnection(false)
        .build();
    ```

1. for java admin client
```java
    PulsarAdmin amdin = PulsarAdmin.builder().serviceHttpUrl("https://broker.example.com:8443")
                .useKeyStoreTls(true)
                .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
                .tlsTrustStorePassword("clientpw")
                .allowTlsInsecureConnection(false)
                .build();
```

## TLS authentication with KeyStore configure

This similar to [TLS authentication with PEM type](security-tls-authentication.md)

### broker authentication config

`broker.conf`

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls

# this should be the CN for one of client keystore.
superUserRoles=admin

# Enable KeyStore type
tlsEnabledWithKeyStore=true
requireTrustedClientCertOnConnect=true

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
brokerClientAuthenticationParameters=keyStoreType:JKS,keyStorePath:/var/private/tls/client.keystore.jks,keyStorePassword:clientpw
# currently websocket not support keystore type
webSocketServiceEnabled=false
```

### client authentication configuring

Besides the TLS encryption configuring. The main work is configuring the KeyStore, which contains a valid CN as client role, for client.

e.g. 
1. for [Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools#pulsar-admin), [`pulsar-perf`](reference-cli-tools#pulsar-perf), and [`pulsar-client`](reference-cli-tools#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

    ```properties
    webServiceUrl=https://broker.example.com:8443/
    brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
    useKeyStoreTls=true
    tlsTrustStoreType=JKS
    tlsTrustStorePath=/var/private/tls/client.truststore.jks
    tlsTrustStorePassword=clientpw
    authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls
    authParams=keyStoreType:JKS,keyStorePath:/var/private/tls/client.keystore.jks,keyStorePassword:clientpw
    ```

1. for java client
    ```java
    import org.apache.pulsar.client.api.PulsarClient;
    
    PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar+ssl://broker.example.com:6651/")
        .enableTls(true)
        .useKeyStoreTls(true)
        .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
        .tlsTrustStorePassword("clientpw")
        .allowTlsInsecureConnection(false)
        .authentication(
                "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls",
                "keyStoreType:JKS,keyStorePath:/var/private/tls/client.keystore.jks,keyStorePassword:clientpw")
        .build();
    ```

1. for java admin client
    ```java
        PulsarAdmin amdin = PulsarAdmin.builder().serviceHttpUrl("https://broker.example.com:8443")
            .useKeyStoreTls(true)
            .tlsTrustStorePath("/var/private/tls/client.truststore.jks")
            .tlsTrustStorePassword("clientpw")
            .allowTlsInsecureConnection(false)
            .authentication(
                   "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls",
                   "keyStoreType:JKS,keyStorePath:/var/private/tls/client.keystore.jks,keyStorePassword:clientpw")
            .build();
    ```

## Enabling TLS Logging

You can enable TLS debug logging at the JVM level by starting the brokers and/or clients with `javax.net.debug` system property. For example:

```shell
-Djavax.net.debug=all
```

You can find more details on this in [Oracle documentation](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html) on
[debugging SSL/TLS connections](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html).
