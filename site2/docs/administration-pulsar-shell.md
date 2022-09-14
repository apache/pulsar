---
id: administration-pulsar-shell
title: Pulsar Shell
sidebar_label: "Pulsar Shell"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


Pulsar shell is a fast and flexible shell for Pulsar cluster management, messaging, and more.
It's great for quickly switching between different clusters, and can modify cluster or tenant configurations in an instant.


## Use case
- Administration: find all the [Admin API](admin-api-overview.md) features under the `admin` command.
- Client: find all the [pulsar-client](reference-cli-tools#pulsar-client.md) features under the `client` command.


## Install Pulsar Shell
Download the tarball from the [download page](https://pulsar.apache.org/download) and extract it.

```shell
wget https://archive.apache.org/dist/pulsar/pulsar-@pulsar:version@/apache-pulsar-shell-@pulsar:version@-bin.tar.gz
tar xzvf apache-pulsar-shell-@pulsar:version@-bin.tar.gz
cd apache-pulsar-shell-@pulsar:version@-bin.tar.gz
```

Now you can enter Pulsar shell's interactive mode:

```shell
./bin/pulsar-shell
Welcome to Pulsar shell!
  Service URL: pulsar://localhost:6650/
  Admin URL: http://localhost:8080/

Type help to get started or try the autocompletion (TAB button).
Type exit or quit to end the shell session.

default(localhost)> 
```


## Connect to your cluster

By default, the shell tries to connect to a local Pulsar instance.
To connect to a different cluster, you have to register the cluster with Pulsar shell. You can do this in a few different ways depending on where your config file is located:

:::note

The configuration file must be a valid `client.conf` file, the same one you use for `pulsar-admin`, `pulsar-client` and other client tools.

:::

````mdx-code-block
<Tabs groupId="shell-config-modes"
  defaultValue="url"
  values={[{"label":"Remote URL","value":"url"},{"label":"File","value":"file"},{"label":"Inline","value":"inline"}]}>
<TabItem value="url">


The `--url` value must point to a valid remote file.

```
default(localhost)> config create --url https://<url_to_my_client.conf> mycluster
```

</TabItem>
<TabItem value="file">

If the file is on your local machine, use the `--file` option.


```
default(localhost)> config create --file ./my-cluster-my-client.conf mycluster
```

</TabItem>
<TabItem value="inline">

You can encode the content of the config to base64 and specify it with the `--value` option.

```
default(localhost)> config create --value "base64:<client.conf_base64_encoded>" mycluster
```


</TabItem>

</Tabs>
````


Once you've configured your cluster, set it as current:

```
default(localhost)> config use mycluster
Welcome to Pulsar shell!
  Service URL: pulsar+ssl://mycluster:6651/
  Admin URL: https://mycluster:8443/

Type help to get started or try the autocompletion (TAB button).
Type exit or quit to end the shell session.

my-cluster(mycluster)> 
```

 
## Run commands sequentially
To run a bunch of admin commands sequentially, you can use Pulsar shell's non-interactive mode.
For example, to set up a new tenant with policies, you would normally need to run multiple `pulsar-admin` commands.

Let's say you want to create a new tenant `new-tenant` with a namespace `new-namespace` in it.
There are multiple ways to do this with Pulsar shell non-interactive mode:

````mdx-code-block
<Tabs groupId="shell-noninteractive-modes"
  defaultValue="single-command"
  values={[{"label":"Single command","value":"single-command"},{"label":"File","value":"file"},{"label":"Unix pipe","value":"pipe"}]}>
<TabItem value="single-command">

Specify a multi-line command with the `-e` option. 

```shell
./bin/pulsar-shell -e "
config use my-cluster
admin tenants create new-tenant 
admin namespaces create new-tenant/new-namespace
" --fail-on-error
```

</TabItem>
<TabItem value="file">

Specify a file command with the `-f` option.


```shell
echo "
# First use my-cluster config
config use my-cluster
# Now it creates a new tenant
admin tenants create new-tenant
# And then it creates a new namespace inside the tenant 
admin namespaces create new-tenant/new-namespace
" > setup-shell.txt

./bin/pulsar-shell -f ./setup-shell.txt --fail-on-error
```

</TabItem>
<TabItem value="pipe">

Make the shell read from the standard input `-` option.

```shell

echo "
# First use my-cluster config
config use my-cluster
# Now it creates a new tenant
admin tenants create new-tenant
# And then it creates a new namespace inside the tenant 
admin namespaces create new-tenant/new-namespace
" > ./bin/pulsar-shell --fail-on-error -

```

</TabItem>

</Tabs>
````