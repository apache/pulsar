---
title: Encryption and Authentication using TLS
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## TLS Overview

With [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) authentication, the server authenticates the client (also called “2-way authentication”).
Since TLS authentication requires TLS encryption, this page shows you how to configure both at the same time.

By default, Apache Pulsar communicates in plain text service url, which means that all data is sent in the clear.
To encrypt communication, it is recommended to configure all the Apache Pulsar components in your deployment to use TLS encryption.

TLS can be configured for encryption or authentication. You may configure just TLS encryption
(by default TLS encryption includes certificate authentication of the server) and independently choose a separate mechanism
for client authentication, e.g. TLS, [Athenz](../../athenz), etc. Note that TLS encryption, technically speaking, already enables
1-way authentication in which the client authenticates the server certificate. So when referring to TLS authentication, it is really
referring to 2-way authentication in which the broker also authenticates the client certificate.

> Note that enabling TLS may have a performance impact due to encryption overhead.

## Creating TLS Certificates

Creating TLS certificates for Pulsar involves creating a [certificate authority](#certificate-authority) (CA), [broker certificate](#broker-certificate), and [client certificate](#client-certificate).

### Certificate authority

The first step is to create the certificate for the CA. The CA will be used to sign both the broker and client certificates, in order to ensure that each party will trust the others.

#### Linux

```bash
$ CA.pl -newca
```

#### macOS

```bash
$ /System/Library/OpenSSL/misc/CA.pl -newca
```

After answering the question prompts, this will store CA-related files in the `./demoCA` directory. Within that directory:

* `demoCA/cacert.pem` is the public certificate. It is meant to be distributed to all parties involved.
* `demoCA/private/cakey.pem` is the private key. This is only needed when signing a new certificate for either broker or clients and it must be safely guarded.

### Broker certificate

Once a CA certificate has been created, you can create certificate requests and sign them with the CA.

The following commands will ask you a few questions and then create the certificates. When asked for the common name, you need to match the hostname of the broker. You could also use a wildcard to match a group of broker hostnames, for example `*.broker.usw.example.com`. This ensures that the same certificate can be reused on multiple machines.

```shell
$ openssl req \
  -newkey rsa:2048 \
  -sha256 \
  -nodes \
  -out broker-cert.csr \
  -outform PEM
```

Convert the key to [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format:

```shell
$ openssl pkcs8 \
  -topk8 \
  -inform PEM \
  -outform PEM \
  -in privkey.pem \
  -out broker-key.pem \
  -nocrypt
```

This will create two broker certificate files named `broker-cert.csr` and `broker-key.pem`. Now you can create the signed certificate:

```shell
$ openssl ca \
  -out broker-cert.pem \
  -infiles broker-cert.csr
```

At this point, you should have a `broker-cert.pem` and `broker-key.pem` file. These will be needed for the broker.

### Client certificate

To create a client certificate, repeat the steps in the previous section, but did create `client-cert.pem` and `client-key.pem` files instead.

For the client common name, you need to use a string that you intend to use as the [role token](#role-tokens) for this client, though it doesn't need to match the client hostname.

## Configure the broker for TLS

To configure a Pulsar {% popover broker %} to use TLS authentication, you'll need to make some changes to the `broker.conf` configuration file, which is located in the `conf` directory of your [Pulsar installation](../../getting-started/LocalCluster).

Add these values to the configuration file (substituting the appropriate certificate paths where necessary):

```properties
# Enable TLS and point the broker to the right certs
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
tlsTrustCertsFilePath=/path/to/cacert.pem

# Enable the TLS auth provider
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls
```

{% include message.html id="broker_conf_doc" %}

## Configure the discovery service

The {% popover discovery %} service used by Pulsar brokers needs to redirect all HTTPS requests, which means that it needs to be trusted by the client as well. Add this configuration in `conf/discovery.conf` in your Pulsar installation:

```properties
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
```

## Configure clients

For more information on Pulsar client authentication using TLS, see the following language-specific docs:

* [Java client](../../clients/Java)
* [C++ client](../../clients/Cpp)

## Configure CLI tools

[Command-line tools](../../reference/CliTools) like [`pulsar-admin`](../../reference/CliTools#pulsar-admin), [`pulsar-perf`](../../reference/CliTools#pulsar-perf), and [`pulsar-client`](../../reference/CliTools#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You'll need to add the following authentication parameters to that file to use TLS with Pulsar's CLI tools:

```properties
serviceUrl=https://broker.example.com:8443/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
authParams=tlsCertFile:/path/to/client-cert.pem,tlsKeyFile:/path/to/client-key.pem
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/cacert.pem
```
