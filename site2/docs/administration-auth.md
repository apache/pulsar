---
id: administration-auth
title: Authentication and authorization in Pulsar
sidebar_label: Authentication and authorization
---

Pulsar supports a pluggable authentication mechanism that Pulsar clients can use to authenticate with {% popover brokers %}. Pulsar can also be configured to support multiple authentication sources.

## Role tokens

In Pulsar, a *role* is a string, like `admin` or `app1`, that can represent a single client or multiple clients. Roles are used to control permission for clients to produce or consume from certain topics, administer the configuration for properties, and more.

The purpose of the [authentication provider](#authentication-providers) is to establish the identity of the client and then assign that client a *role token*. This role token is then used to determine what the client is authorized to do.

## Authentication providers

Out of the box, Pulsar supports two authentication providers:

* [TLS client auth](#tls-client-auth)
* [Athenz](#athenz)

### TLS client auth

In addition to providing connection encryption between Pulsar clients and {% popover brokers %}, [Transport Layer Security](https://en.wikipedia.org/wiki/Transport_Layer_Security) (TLS) can be used to identify clients through a certificate signed by a trusted certificate authority.

#### Creating certificates

Creating TLS certificates for Pulsar involves creating a [certificate authority](#certificate-authority) (CA), [broker certificate](#broker-certificate), and [client certificate](#client-certificate).

##### Certificate authority

The first step is to create the certificate for the CA. The CA will be used to sign both the broker and client certificates, in order to ensure that each party will trust the others.

###### Linux

```bash
$ CA.pl -newca
```

###### macOS

```bash
$ /System/Library/OpenSSL/misc/CA.pl -newca
```

After answering the question prompts, this will store CA-related files in the `./demoCA` directory. Within that directory:

* `demoCA/cacert.pem` is the public certificate. It is meant to be distributed to all parties involved.
* `demoCA/private/cakey.pem` is the private key. This is only needed when signing a new certificate for either broker or clients and it must be safely guarded.

##### Broker certificate

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

##### Client certificate

To create a client certificate, repeat the steps in the previous section, but did create `client-cert.pem` and `client-key.pem` files instead.

For the client common name, you need to use a string that you intend to use as the [role token](#role-tokens) for this client, though it doesn't need to match the client hostname.

#### Configure the broker for TLS

To configure a Pulsar broker to use TLS authentication, you'll need to make some changes to the `broker.conf` configuration file, which is located in the `conf` directory of your [Pulsar installation](getting-started-standalone.md).

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

> A full listing of parameters available in the `conf/broker.conf` file, as well as the default values for those parameters, can be found in Broker Configuration.


#### Configure the discovery service

The discovery service used by Pulsar brokers needs to redirect all HTTPS requests, which means that it needs to be trusted by the client as well. Add this configuration in `conf/discovery.conf` in your Pulsar installation:

```properties
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
```

#### Configure clients

For more information on Pulsar client authentication using TLS, see the following language-specific docs:

* [Java client](client-libraries-java.md)
* [C++ client](client-libraries-cpp.md)

#### Configure CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-pulsar-admin.md), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You'll need to add the following authentication parameters to that file to use TLS with Pulsar's CLI tools:

```properties
serviceUrl=https://broker.example.com:8443/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
authParams=tlsCertFile:/path/to/client-cert.pem,tlsKeyFile:/path/to/client-key.pem
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/cacert.pem
```

### Athenz

[Athenz](https://github.com/yahoo/athenz) is a role-based authentication/authorization system. In Pulsar, Athenz [role tokens](#role-tokens) (aka *z-tokens*) can be used to establish the identify of the client.

#### Athenz authentication settings

In a [decentralized Athenz system](https://github.com/yahoo/athenz/blob/master/docs/dev_decentralized_access.md) there is both an [authori**Z**ation **M**anagement **S**ystem](https://github.com/yahoo/athenz/blob/master/docs/setup_zms.md) (ZMS) server and an  [authori**Z**ation **T**oken **S**ystem](https://github.com/yahoo/athenz/blob/master/docs/setup_zts.md) (ZTS) server.

To begin, you need to set up Athenz service access control. You should create domains for the *provider* (which provides some resources to other services with some authentication/authorization policies) and the *tenant* (which is provisioned to access some resources in a provider). In this case, the provider corresponds to the Pulsar service itself and the tenant corresponds to each application using Pulsar (typically, a property in Pulsar).

##### Create the tenant domain and service

On the tenant side, you need to:

1. Create a domain, such as `shopping`
2. Generate a private/public key pair
3. Create a service, such as `some_app`, on the domain with the public key

Note that the private key generated in step 2 needs to be specified when the Pulsar client connects to the broker (see client configuration examples for [Java](client-libraries-java.md#tls-authentication) and [C++](client-libraries-cpp.md#tls-authentication)).

For more specific steps involving the Athenz UI, please refer to [this doc](https://github.com/yahoo/athenz/blob/master/docs/example_service_athenz_setup.md#client-tenant-domain).

##### Create the provider domain and add the tenant service to some role members

On the provider side, you need to:

1. Create a domain, such as `pulsar`
2. Create a role
3. Add the tenant service to members of the role

Note that in step 2 any action and resource can be specified since they are not used on Pulsar. In other words, Pulsar uses the Athenz role token only for authentication, *not* for authorization.

For more specific steps involving UI, please refer to [this doc](https://github.com/yahoo/athenz/blob/master/docs/example_service_athenz_setup.md#server-provider-domain).

#### Configure the broker for Athenz

{% include message.html id="tls_role_tokens" %}

> #### TLS encryption strongly recommended
> Please note that using TLS encryption is strongly recommended when using Athenz as an authentication provider, as it can protect role tokens from being intercepted and reused (see also [this doc]()).


In the `conf/broker.conf` configuration file in your Pulsar installation, you need to provide the class name of the Athenz authentication provider as well as a comma-separated list of provider domain names.

```properties
# Add the Athenz auth provider
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderAthenz
athenzDomainNames=pulsar

# Enable TLS
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
```

> A full listing of parameters available in the conf/broker.conf file, as well as the default values for those parameters, can be found in [Broker Configuration]().

#### Configure clients for Athenz

For more information on Pulsar client authentication using Athenz, see the following language-specific docs:

* [Java client](client-libraries-java.md#athenz)

#### Configure CLI tools for Athenz

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-pulsar-admin.md), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You’ll need to add the following authentication parameters to that file to use Athenz with Pulsar’s CLI tools:

```properties
# URL for the broker
serviceUrl=https://broker.example.com:8443/

# Set Athenz auth plugin and its parameters
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationAthenz
authParams={"tenantDomain":"shopping","tenantService":"some_app","providerDomain":"pulsar","privateKey":"file:///path/to/private.pem","keyId":"v1"}

# Enable TLS
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/cacert.pem
```

## Authorization

In Pulsar, the [authentication provider](#authentication-providers) is charged with properly identifying clients and associating them with [role tokens](#role-tokens). *Authorization* is the process that determines *what* clients are able to do.

Authorization in Pulsar is managed at the tenant level, which means that you can have multiple authorization schemes active in a single Pulsar instance. You could, for example, create a `shopping` tenant that has one set of [roles](#role-tokens) and applies to a shopping application used by your company, while an `inventory` tenant would be used only by an inventory application.

> When working with properties, you can specify which of your Pulsar clusters your tenant is allowed to use. This enables you to also have cluster-level authorization schemes.

## Creating a new tenant

A Pulsar tenant is typically provisioned by Pulsar {% popover instance %} administrators or by some kind of self-service portal.

Properties are managed using the [`pulsar-admin`](reference-pulsar-admin.md) tool. Here's an example property creation command:

```shell
$ bin/pulsar-admin tenants create my-tenant \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east
```

This command will create a new property `my-tenant` that will be allowed to use the clusters `us-west` and `us-east`.

A client that successfully identified itself as having the role `my-admin-role` would then be allowed to perform all administrative tasks on this tenant.

The structure of topic names in Pulsar reflects the hierarchy between tenants, clusters, and [namespaces](#managing-namespaces):

```http
persistent://tenant/namespace/topic
```

## Managing permissions

Permissions in Pulsar are managed at the namespace level (that is, within tenants and clusters).

### Grant permissions

You can grant permissions to specific roles for lists of operations such as `produce` and `consume`.

#### pulsar-admin

Use the [`grant-permission`](reference-pulsar-admin.md#namespaces-grant-permission) subcommand and specify a namespace, actions using the `--actions` flag, and a role using the `--role` flag:

```shell
$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
  --actions produce,consume \
  --role admin10
```

Wildcard authorization can be performed when `authorizationAllowWildcardsMatching` is set to `true` in `broker.conf`.

e.g.
```shell
$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
                        --actions produce,consume \
                        --role 'my.role.*'
```

Then, roles `my.role.1`, `my.role.2`, `my.role.foo`, `my.role.bar`, etc. can produce and consume.  

```shell
$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
                        --actions produce,consume \
                        --role '*.role.my'
```

Then, roles `1.role.my`, `2.role.my`, `foo.role.my`, `bar.role.my`, etc. can produce and consume.

**Note**: A wildcard matching works at **the beginning or end of the role name only**.

e.g.
```shell
$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
                        --actions produce,consume \
                        --role 'my.*.role'
```

In this case, only the role `my.*.role` has permissions.  
Roles `my.1.role`, `my.2.role`, `my.foo.role`, `my.bar.role`, etc. **cannot** produce and consume.

#### REST API

```http
POST /admin/v2/namespaces/:tenant/:namespace/permissions/:role
```
[More info](reference-rest-api.md#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions));
```

### Get permission

You can see which permissions have been granted to which roles in a namespace.

#### pulsar-admin

Use the [`permissions`](reference-pulsar-admin.md#namespaces-permissions) subcommand and specify a namespace:

```shell
$ pulsar-admin namespaces permissions test-tenant/ns1
{
  "admin10": [
    "produce",
    "consume"
  ]
}   
```

#### REST API

```http
GET /admin/v2/namespaces/:tenant/:namespace/permissions
```

[More info](reference-rest-api.md#/admin/namespaces/:property/:cluster/:namespace/permissions)

#### Java

```java
admin.namespaces().getPermissions(namespace);
```

### Revoke permissions

You can revoke permissions from specific roles, which means that those roles will no longer have access to the specified namespace.

#### pulsar-admin

Use the [`revoke-permission`](reference-pulsar-admin.md#revoke-permission) subcommand and specify a namespace and a role using the `--role` flag:

```shell
$ pulsar-admin namespaces revoke-permission test-tenant/ns1 \
  --role admin10
```

#### REST API

```http
DELETE /admin/v2/namespaces/:tenant/:namespace/permissions/:role
```

[More info](reference-rest-api.md#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
admin.namespaces().revokePermissionsOnNamespace(namespace, role);
```


## Superusers

In Pulsar you can assign certain roles to be *superusers* of the system. A superuser is allowed to perform all administrative tasks on all properties and namespaces, as well as to publish and subscribe to all topics.

Superusers are configured in the broker configuration file in [`conf/broker.conf`](reference-configuration.md#broker) configuration file, using the [`superUserRoles`](reference-configuration.md#broker-superUserRoles) parameter:

```properties
superUserRoles=my-super-user-1,my-super-user-2
```

> A full listing of parameters available in the `conf/broker.conf` file, as well as the default values for those parameters, can be found in [Broker Configuration]().

Typically, superuser roles are used for administrators and clients but also for broker-to-broker authorization. When using [geo-replication](administration-geo.md), every broker
needs to be able to publish to other clusters' topics.

## Pulsar admin authentication

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1:value1";
boolean useTls = false;
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setUseTls(useTls);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin(url, config);
```

To use TLS:

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1:value1";
boolean useTls = false;
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setUseTls(useTls);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin(url, config);
```
