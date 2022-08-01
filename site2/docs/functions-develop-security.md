---
id: functions-develop-security
title: Enable security on functions
sidebar_label: "Enable security on functions"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

## Prerequisites

If you want to enable security on functions, you need to [enable security settings](functions-worker.md#enable-security-settings) on function workers first.


## Configure function workers

To use the secret APIs from the context, you need to set the following two parameters for function workers.
* `secretsProviderConfiguratorClassName`
* `secretsProviderConfiguratorConfig`

Pulsar Functions provided two types of `SecretsProviderConfigurator` implementation and both can be used as the value of `secretsProviderConfiguratorClassName` directly:
* `org.apache.pulsar.functions.secretsproviderconfigurator.DefaultSecretsProviderConfigurator`: This is a barebones version of a secrets provider which wires in `ClearTextSecretsProvider` to the function instances.
* `org.apache.pulsar.functions.secretsproviderconfigurator.KubernetesSecretsProviderConfigurator`: This is used by default for running in Kubernetes and it uses kubernetes built-in secrets and bind them as environment variables (via `EnvironmentBasedSecretsProvider`) within the function container to ensure that the secrets are available to the function at runtime. 

Function workers use the `org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator` interface to choose the `SecretsProvider` class name and its associated configurations at the time of starting the function instances.

Function instances use the `org.apache.pulsar.functions.secretsprovider.SecretsProvider` interface to fetch the secrets. The implementation that `SecretsProvider` uses is determined by `SecretsProviderConfigurator`.

You can also implemet your own `SecretsProviderConfigurator` if you want to use different `SecretsProvider` for function instances.

:::note

Currently, only Java and Python runtime support `SecretsProvider`. The Java and Python Runtime have the following two providers:
- ClearTextSecretsProvider (default for `DefaultSecretsProviderConfigurator`)
- EnvironmentBasedSecretsProvider (default for `KubernetesSecretsProviderConfigurator`)

:::

## Get the secret

Once `SecretsProviderConfigurator` is set, you can get the secret using the [`Context`](functions-concepts.md#context) object as follows.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"}]}>
<TabItem value="Java">

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class GetSecretValueFunction implements Function<String, Void> {

    @Override
    public Void process(String input, Context context) throws Exception {
        Logger LOG = context.getLogger();
        String secretValue = context.getSecret(input);

        if (!secretValue.isEmpty()) {
            LOG.info("The secret {} has value {}", intput, secretValue);
        } else {
            LOG.warn("No secret with key {}", input);
        }

        return null;
    }
}

```

</TabItem>
<TabItem value="Python">

```python

from pulsar import Function

class GetSecretValueFunction(Function):
    def process(self, input, context):
        logger = context.get_logger()
        secret_value = context.get_secret(input)
        if secret_provider is None:
            logger.warn('No secret with key {0} '.format(input))
        else:
            logger.info("The secret {0} has value {1}".format(input, secret_value))

```

</TabItem>
</Tabs>
````
