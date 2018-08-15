---
title: Authentication using TLS
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

## TLS Authentication Overview

TLS authentication is an extension of [TLS transport encryption](../tls-transport), but instead of only servers having keys and certs which the client uses the verify the server's identity, clients also have keys and certs which the server uses to verify the client's identity. You must have TLS transport encryption configured on your cluster before you can use TLS authentication. This guide assumes you already have TLS transport encryption configured.

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
      -key admin.key.pem -new -sha256 -out admin.cert.pem
```

Sign with request with the certificate authority. Note that that client certs uses the **usr_cert** extension, which allows the cert to be used for client authentication.

```bash
$ openssl ca -config openssl.cnf -extensions usr_cert \
      -days 1000 -notext -md sha256 \
      -in admin.csr.pem -out admin.cert.pem
```

This will give you a cert, `admin.cert.pem`, and a key, `admin.key-pk8.pem`, which, with `ca.cert.pem`, can be used by clients to authenticate themselves to brokers and proxies as the role token ``admin``.

## Enabling TLS Authentication ...

### ... on Brokers

To configure brokers to authenticate clients, put the following in `broker.conf`, alongside [the configuration to enable tls transport](../tls-transport#broker-configuration):

```properties
# Configuration to enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls
```

### ... on Proxies

To configure proxies to authenticate clients, put the folling in `proxy.conf`, alongside [the configuration to enable tls transport](../tls-transport#proxy-configuration):

The proxy should have its own client key pair for connecting to brokers. The role token for this key pair should be configured in the ``proxyRoles`` of the brokers. See the [authorization guide](../authorization) for more details.

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

[Command-line tools](../../reference/CliTools) like [`pulsar-admin`](../../reference/CliTools#pulsar-admin), [`pulsar-perf`](../../reference/CliTools#pulsar-perf), and [`pulsar-client`](../../reference/CliTools#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You'll need to add the following parameters to that file to use TLS authentication with Pulsar's CLI tools:

```properties
webServiceUrl=https://broker.example.com:8443/
brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
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
config.setTlsTrustCertsFilePath("/path/to/ca.cert.pem");
config.setTlsAllowInsecureConnection(false);

pulsar::AuthenticationPtr auth = pulsar::AuthTls::create("/path/to/my-role.cert.pem",
                                                         "/path/to/my-role.key-pk8.pem")
config.setAuth(auth);

pulsar::Client client("pulsar+ssl://broker.example.com:6651/", config);
```

