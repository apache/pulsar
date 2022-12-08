---
id: security-athenz
title: Authentication using Athenz
sidebar_label: "Authentication using Athenz"
---

[Athenz](https://github.com/AthenZ/athenz) is a role-based authentication/authorization system. In Pulsar, you can use Athenz role tokens (also known as *z-tokens*) to establish the identity of the client.

A [decentralized Athenz system](https://github.com/AthenZ/athenz/blob/master/docs/decent_authz_flow.md) contains an [authori**Z**ation **M**anagement **S**ystem](https://github.com/AthenZ/athenz/blob/master/docs/setup_zms.md) (ZMS) server and an [authori**Z**ation **T**oken **S**ystem](https://github.com/AthenZ/athenz/blob/master/docs/setup_zts) (ZTS) server.

## Prerequisites

To begin, you need to set up Athenz service access control by creating domains for the *provider* (which provides some resources to other services with some authentication/authorization policies) and the *tenant* (which is provisioned to access some resources in a provider). In this case, the provider corresponds to the Pulsar service itself and the tenant corresponds to each application using Pulsar (typically, a [tenant](reference-terminology.md#tenant) in Pulsar).

### Create a tenant domain and service

On the tenant side, do the followings:

1. Create a domain, such as `shopping`.
2. Generate a private/public key pair.
3. Create a service, such as `some_app`, on the domain with the public key.

Note that you need to specify the private key generated in step 2 when the Pulsar client connects to the broker.

For more specific steps involving the Athenz UI, refer to [Example Service Access Control Setup](https://github.com/AthenZ/athenz/blob/master/docs/example_service_athenz_setup.md#client-tenant-domain).

### Create a provider domain and add the tenant service to role members

On the provider side, you need to do the following things:

1. Create a domain, such as `pulsar`.
2. Create a role.
3. Add the tenant service to the members of the role.

Note that you can specify any action and resource in step 2 since they are not used on Pulsar. In other words, Pulsar uses the Athenz role token only for authentication, *not* for authorization.

For more specific steps involving the Athenz UI, refer to [Example Service Access Control Setup](https://github.com/AthenZ/athenz/blob/master/docs/example_service_athenz_setup.md#server-provider-domain).

## Enable Athenz authentication on brokers/proxies

To configure brokers/proxies to authenticate clients using Authenz, add the following parameters to the `conf/broker.conf` and the `conf/proxy.conf` files and provide the class name of the Athenz authentication provider as well as a comma-separated list of provider domain names. If you use a standalone Pulsar, you need to add these parameters to the `conf/standalone.conf` file.

```properties
# Add the Athenz auth provider
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderAthenz
athenzDomainNames=pulsar

# Authentication settings of the broker itself. Used when the broker connects to other brokers, or when the proxy connects to brokers, either in same or other clusters
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationAthenz
brokerClientAuthenticationParameters={"tenantDomain":"shopping","tenantService":"some_app","providerDomain":"pulsar","privateKey":"file:///path/to/private.pem","keyId":"v1"}
```

## Configure Athenz authentication in Pulsar clients

To use Athenz as an authentication provider, you need to provide values for four parameters in a hash:
* `tenantDomain`
* `tenantService`
* `providerDomain`
* `privateKey`

:::tip

The `privateKey` parameter supports the following three pattern formats:
* `file:///path/to/file`
* `file:/path/to/file`
* `data:application/x-pem-file;base64,<base64-encoded value>`

:::

You can also set an optional `keyId`. The following is an example.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"},{"label":"Go","value":"Go"}]}>
<TabItem value="Java">

```java
Map<String, String> authParams = new HashMap();
authParams.put("ztsUrl", "http://localhost:9998");
authParams.put("tenantDomain", "shopping"); // Tenant domain name
authParams.put("tenantService", "some_app"); // Tenant service name
authParams.put("providerDomain", "pulsar"); // Provider domain name
authParams.put("privateKey", "file:///path/to/private.pem"); // Tenant private key path
authParams.put("keyId", "v1"); // Key id for the tenant private key (optional, default: "0")

Authentication athenzAuth = AuthenticationFactory
        .create(AuthenticationAthenz.class.getName(), authParams);

PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://my-broker.com:6650")
        .authentication(athenzAuth)
        .build();
```

</TabItem>
<TabItem value="Python">

```python
authPlugin = "athenz"
authParams = """
{
"tenantDomain": "shopping",
"tenantService": "some_app",
"providerDomain": "pulsar",
"privateKey": "file:///path/to/private.pem",
"ztsUrl": "http://localhost:9998"
}
"""

client = Client(
    "pulsar://my-broker.com:6650",
    authentication=Authentication(authPlugin, authParams),
)
```

</TabItem>
<TabItem value="C++">

```cpp
std::string params = R"({
        "tenantDomain": "shopping",
        "tenantService": "some_app",
        "providerDomain": "pulsar",
        "privateKey": "file:///path/to/private.pem",
        "ztsUrl": "http://localhost:9998"
    })";
pulsar::AuthenticationPtr auth = pulsar::AuthAthenz::create(params);
ClientConfiguration config = ClientConfiguration();
config.setAuth(auth);
Client client("pulsar://my-broker.com:6650", config);
```

</TabItem>
<TabItem value="Node.js">

```javascript
const auth = new Pulsar.AuthenticationAthenz({
    tenantDomain: "shopping",
    tenantService: "some_app",
    providerDomain: "pulsar",
    privateKey: "file:///path/to/private.pem",
    ztsUrl: "http://localhost:9998"
});

const client = new Pulsar.Client({
    serviceUrl: 'pulsar://my-broker.com:6650',
    authentication: auth
});
```

</TabItem>
<TabItem value="Go">

```go
provider := pulsar.NewAuthenticationAthenz(
		"pulsar",
		"shopping",
		"some_app",
		"file:///path/to/private.pem",
		"v1",
		"",
		"http://localhost:9998")
client, err := pulsarNewClient(ClientOptions{
		URL:                   "pulsar://my-broker.com:6650",
		Authentication:        basicAuth,
	})
```

</TabItem>
</Tabs>
````

## Configure Athenz authentication in CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](/tools/pulsar-admin/), [`pulsar-perf`](reference-cli-tools.md), and [`pulsar-client`](reference-cli-tools.md) use the `conf/client.conf` config file in a Pulsar installation.

You need to add the following authentication parameters to the `conf/client.conf` config file to use Athenz with CLI tools of Pulsar:

```properties
# URL for the broker
serviceUrl=http://broker.example.com:8080

# Set Athenz auth plugin and its parameters
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationAthenz
authParams={"tenantDomain":"shopping","tenantService":"some_app","providerDomain":"pulsar","privateKey":"file:///path/to/private.pem","keyId":"v1"}
```