---
id: administration-pulsar-shell
title: Pulsar Shell
sidebar_label: "Pulsar Shell"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


Pulsar shell is a fast and flexible shell for Pulsar clusters management, producing and consuming messages and more. 
It is great for quickly switch between different clusters and modify cluster or tenant configurations in an instant.


# Features
- Administration: under `admin` command you can find all the [Admin API](admin-api-overview.md) features.
- Client: under `client` command you can find all the [pulsar-client](reference-cli-tools#pulsar-client.md) features.


# Installation

Download the tarball from the [download page](https://pulsar.apache.org/download) and extract it.

```shell
wget https://archive.apache.org/dist/pulsar/pulsar-@pulsar:version@/apache-pulsar-shell-@pulsar:version@-bin.tar.gz
tar xzvf apache-pulsar-shell-@pulsar:version@-bin.tar.gz
cd apache-pulsar-shell-@pulsar:version@-bin.tar.gz
```

Now you can enter the interactive mode:

```shell
$ ./bin/pulsar-shell
Welcome to Pulsar shell!
  Service URL: pulsar://localhost:6650/
  Admin URL: http://localhost:8080/

Type help to get started or try the autocompletion (TAB button).
Type exit or quit to end the shell session.

default(localhost)> 
```


# Connect to your cluster

By default, the shell will try to connect to a local Pulsar instance.
In order to connect to a different cluster you have to register it. You can do in different ways depending on where your config file is located:

> The configuration file must be a valid `client.conf` file, the same one you use for `pulsar-admin`, `pulsar-client` and other client tools.

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

If the file is on your local machine you can use the `--file` option.


```
default(localhost)> config create --file ./my-cluster-my-client.conf mycluster
```

</TabItem>
<TabItem value="inline">

You can encode the content of the config to base64 and specify it into the `--value` option.

```
default(localhost)> config create --value "base64:<client.conf_base64_encoded>" mycluster
```


</TabItem>

</Tabs>
````


Once you configured your cluster, you have to set it as current:

```
default(localhost)> config use mycluster
Welcome to Pulsar shell!
  Service URL: pulsar+ssl://mycluster:6651/
  Admin URL: https://mycluster:8443/

Type help to get started or try the autocompletion (TAB button).
Type exit or quit to end the shell session.

my-cluster(mycluster)> 
```

 
# Non interactive mode
Pulsar shell non interactive mode is great for running bunch of admin commands sequentially.
For instance, you may need to run several commands to setup a new tenant with policies; normally you can do it using `pulsar-admin` command.
Pulsar shell, due to its nature, is faster and allows you to save time.

Let's say you want to create a new tenant `new-tenant` with a namespace `new-namespace` in it.
To run the non-interactive mode you have multiple ways: 

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

Make the shell reading from the stadanrd input `-` option.

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