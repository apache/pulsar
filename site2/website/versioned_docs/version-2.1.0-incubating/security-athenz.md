---
id: version-2.1.0-incubating-security-athenz
title: Authentication using Athenz
sidebar_label: Authentication using Athenz
original_id: security-athenz
---

[Athenz](https://github.com/AthenZ/athenz) is a role-based authentication/authorization system. In Pulsar, Athenz role tokens (aka *z-tokens*) can be used to establish the identify of the client.

## Athenz authentication settings

In a [decentralized Athenz system](https://github.com/AthenZ/athenz/blob/master/docs/decent_authz_flow.md) there is both an [authori**Z**ation **M**anagement **S**ystem](https://github.com/AthenZ/athenz/blob/master/docs/setup_zms.md) (ZMS) server and an  [authori**Z**ation **T**oken **S**ystem](https://github.com/AthenZ/athenz/blob/master/docs/setup_zts.md) (ZTS) server.

To begin, you need to set up Athenz service access control. You should create domains for the *provider* (which provides some resources to other services with some authentication/authorization policies) and the *tenant* (which is provisioned to access some resources in a provider). In this case, the provider corresponds to the Pulsar service itself and the tenant corresponds to each application using Pulsar (typically, a [tenant](reference-terminology.md#tenant) in Pulsar).

### Create the tenant domain and service

On the [tenant](reference-terminology.md#tenant) side, you need to:

1. Create a domain, such as `shopping`
2. Generate a private/public key pair
3. Create a service, such as `some_app`, on the domain with the public key

Note that the private key generated in step 2 needs to be specified when the Pulsar client connects to the [broker](reference-terminology.md#broker) (see client configuration examples for [Java](client-libraries-java.md#tls-authentication) and [C++](client-libraries-cpp.md#tls-authentication)).

For more specific steps involving the Athenz UI, please refer to [this doc](https://github.com/AthenZ/athenz/blob/master/docs/example_service_athenz_setup.md#client-tenant-domain).

### Create the provider domain and add the tenant service to some role members

On the provider side, you need to:

1. Create a domain, such as `pulsar`
2. Create a role
3. Add the tenant service to members of the role

Note that in step 2 any action and resource can be specified since they are not used on Pulsar. In other words, Pulsar uses the Athenz role token only for authentication, *not* for authorization.

For more specific steps involving UI, please refer to [this doc](https://github.com/AthenZ/athenz/blob/master/docs/example_service_athenz_setup.md#server-provider-domain).

## Configure the broker for Athenz

> ### TLS encryption strongly recommended
>
> Please note that using TLS encryption is strongly recommended when using Athenz as an authentication provider,
> as it can protect role tokens from being intercepted and reused (see also [this doc](https://github.com/AthenZ/athenz/blob/master/docs/data_model.md)).

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

# Authentication settings of the broker itself. Used when the broker connects to other brokers, either in same or other clusters
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationAthenz
brokerClientAuthenticationParameters={"tenantDomain":"shopping","tenantService":"some_app","providerDomain":"pulsar","privateKey":"file:///path/to/private.pem","keyId":"v1"}
```

> A full listing of parameters available in the `conf/broker.conf` file, as well as the default
> values for those parameters, can be found in [Broker Configuration](reference-configuration.md#broker).

## Configure clients for Athenz

For more information on Pulsar client authentication using Athenz, see the following language-specific docs:

* [Java client](client-libraries-java.md#athenz)

## Configure CLI tools for Athenz

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
