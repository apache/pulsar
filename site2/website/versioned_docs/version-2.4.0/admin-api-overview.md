---
id: admin-api-overview
title: The Pulsar admin interface
sidebar_label: "Overview"
original_id: admin-api-overview
---

The Pulsar admin interface enables you to manage all of the important entities in a Pulsar [instance](reference-terminology.md#instance), such as [tenants](reference-terminology.md#tenant), [topics](reference-terminology.md#topic), and [namespaces](reference-terminology.md#namespace).

You can currently interact with the admin interface via:

- Making HTTP calls against the admin {@inject: rest:REST:/} API provided by Pulsar [brokers](reference-terminology.md#broker). For some restful apis, they might be redirected to topic owner brokers for serving
   with [`307 Temporary Redirect`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/307), hence the HTTP callers should handle `307 Temporary Redirect`. If you are using `curl`, you should specify `-L`
   to handle redirections.
- The `pulsar-admin` CLI tool, which is available in the `bin` folder of your [Pulsar installation](getting-started-standalone):

```shell

$ bin/pulsar-admin

```

Full documentation for this tool can be found in the [Pulsar command-line tools](reference-pulsar-admin) doc.

- A Java client interface.

> #### The REST API is the admin interface
> Under the hood, both the `pulsar-admin` CLI tool and the Java client both use the REST API. If you’d like to implement your own admin interface client, you should use the REST API as well. Full documentation can be found here.

In this document, examples from each of the three available interfaces will be shown.

## Admin setup

Each of Pulsar's three admin interfaces---the [`pulsar-admin`](reference-pulsar-admin) CLI tool, the [Java admin API](/api/admin), and the {@inject: rest:REST:/} API ---requires some special setup if you have [authentication](security-overview.md#authentication-providers) enabled in your Pulsar [instance](reference-terminology.md#instance).

### pulsar-admin

If you have [authentication](security-overview.md#authentication-providers) enabled, you will need to provide an auth configuration to use the [`pulsar-admin`](reference-pulsar-admin) tool. By default, the configuration for the `pulsar-admin` tool is found in the [`conf/client.conf`](reference-configuration.md#client) file. Here are the available parameters:

|Name|Description|Default|
|----|-----------|-------|
|webServiceUrl|The web URL for the cluster.|http://localhost:8080/|
|brokerServiceUrl|The Pulsar protocol URL for the cluster.|pulsar://localhost:6650/|
|authPlugin|The authentication plugin.| |
|authParams|The authentication parameters for the cluster, as a comma-separated string.| |
|useTls|Whether or not TLS authentication will be enforced in the cluster.|false|
|tlsAllowInsecureConnection|Accept untrusted TLS certificate from client.|false|
|tlsTrustCertsFilePath|Path for the trusted TLS certificate file.| |

### REST API

You can find documentation for the REST API exposed by Pulsar [brokers](reference-terminology.md#broker) in this reference {@inject: rest:document:/}.

### Java admin client

To use the Java admin API, instantiate a {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object, specifying a URL for a Pulsar [broker](reference-terminology.md#broker) and a {@inject: javadoc:ClientConfiguration:/admin/org/apache/pulsar/client/admin/ClientConfiguration}. Here's a minimal example using `localhost`:

```java

URL url = new URL("http://localhost:8080");
// Pass auth-plugin class fully-qualified name if Pulsar-security enabled
String authPluginClassName = "com.org.MyAuthPluginClass"; 
// Pass auth-param if auth-plugin class requires it
String authParams = "param1=value1";
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

If you have multiple brokers to use, you can use multi-host like Pulsar service. For example,

```java

URL url = new URL("http://localhost:8080,localhost:8081,localhost:8082");
// Pass auth-plugin class fully-qualified name if Pulsar-security is enabled.
String authPluginClassName = "com.org.MyAuthPluginClass"; 
// Pass auth-param if auth-plugin class requires it
String authParams = "param1=value1";
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

