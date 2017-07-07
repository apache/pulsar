## Admin setup

If you have [authentication](../../admin/Authz#authentication-providers) enabled in your Pulsar {% popover instance %}, then you will need to perform a few authentication-related setup steps to use the Pulsar [admin interface](../../admin/AdminInterface). Those steps will vary depending on if you're using the [`pulsar-admin` command-line interface](#the-pulsar-admin-command-line-tool), the [REST API](#rest-api), or the [Java admin client](#java-admin-client).

Before you get started using Pulsar's admin interface, you will need to complete a few setup steps that vary depending on whether you're using the [`pulsar-admin` CLI tool](#pulsar-admin-cli-tool), [Java API](#java-api), or [REST API](#rest-api) directly.

### The `pulsar-admin` command-line tool

If a Pulsar {% popover broker %} has [authentication](../../admin/Authz#authentication-providers) enabled, you will need to provide an auth configuration to use the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool. By default, the configuration for the `pulsar-admin` tool is found in the [`conf/client.conf`](../../reference/Configuration#client) file. Here are the available parameters:

{% include config.html id="client" %}

### REST API

You can find documentation for the REST API exposed by Pulsar {% popover brokers %} in [this reference document](../../reference/RestApi).

### Java admin client

To use the Java admin API, instantiate a {% javadoc PulsarAdmin admin com.yahoo.pulsar.client.admin.PulsarAdmin %} object, specifying a URL for a Pulsar {% popover broker %} and a {% javadoc ClientConfiguration admin com.yahoo.pulsar.client.admin.ClientConfiguration %}. Here's a minimal example using `localhost`:

```java
URL url = new URL("http://localhost:8080");
String authPluginClassName = "com.org.MyAuthPluginClass"; //Pass auth-plugin class fully-qualified name if Pulsar-security enabled
String authParams = "param1=value1";//Pass auth-param if auth-plugin class requires it
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
