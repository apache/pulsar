---
id: admin-api-packages
title: Manage packages
sidebar_label: "Packages"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


> **Important**
>
> This page only shows **some frequently used operations**.
>
> - For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](/tools/pulsar-admin/).
> 
> - For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.
> 
> - For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

Package managers or package-management systems automatically manage packages in a consistent manner. These tools simplify the installation tasks, upgrade process, and deletion operations for users. A package is a minimal unit that a package manager deals with. In Pulsar, packages are organized at the tenant- and namespace-level to manage Pulsar Functions and Pulsar IO connectors (i.e., source and sink).

## What is a package?

A package is a set of elements that the user would like to reuse in later operations. In Pulsar, a package can be a group of functions, sources, and sinks. You can define a package according to your needs. 

The package management system in Pulsar stores the data and metadata of each package (as shown in the table below) and tracks the package versions. 

|Metadata|Description|
|--|--|
|description|The description of the package.|
|contact|The contact information of a package. For example, an email address of the developer team.|
|create_time|The time when the package is created.|
|modification_time|The time when the package is lastly modified.|
|properties|A user-defined key/value map to store other information.|

## How to use a package

Packages can efficiently use the same set of functions and IO connectors. For example, you can use the same function, source, and sink in multiple namespaces. The main steps are:

1. Create a package in the package manager by providing the following information: type, tenant, namespace, package name, and version.

   |Component|Description|
   |-|-|
   |type|Specify one of the supported package types: function, sink and source.|
   |tenant|Specify the tenant where you want to create the package.|
   |namespace|Specify the namespace where you want to create the package.|
   |name|Specify the complete name of the package, using the format `<tenant>/<namespace>/<package name>`.|
   |version|Specify the version of the package using the format `MajorVerion.MinorVersion` in numerals.|

   The information you provide creates a URL for a package, in the format `<type>://<tenant>/<namespace>/<package name>/<version>`.

2. Upload the elements to the package, i.e., the functions, sources, and sinks that you want to use across namespaces.

3. Apply permissions to this package from various namespaces.

Now, you can use the elements you defined in the package by calling this package from within the package manager. The package manager locates it by the URL. For example,

```

sink://public/default/mysql-sink@1.0
function://my-tenant/my-ns/my-function@0.1
source://my-tenant/my-ns/mysql-cdc-source@2.3

```

## Package management in Pulsar

You can use the command line tools, REST API, or the Java client to manage your package resources in Pulsar. More specifically, you can use these tools to [upload](#upload-a-package), [download](#download-a-package), and [delete](#delete-a-package) a package, [get the metadata](#get-the-metadata-of-a-package) and [update the metadata](#update-the-metadata-of-a-package) of a package, [get the versions](#list-all-versions-of-a-package) of a package, and [get all packages of a specific type under a namespace](#list-all-packages-of-a-specific-type-under-a-namespace).

To use package management service, ensure that the package management service has been enabled in your cluster by setting the following properties in `broker.conf`.

> Note: Package management service is not enabled by default.

```yaml

enablePackagesManagement=true
packagesManagementStorageProvider=org.apache.pulsar.packages.management.storage.bookkeeper.BookKeeperPackagesStorageProvider
packagesReplicas=1
packagesManagementLedgerRootPath=/ledgers

```

### Upload a package

You can use the following commands to upload a package.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages upload function://public/default/example@v0.1 --path package-file --description package-description

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version}

</TabItem>
<TabItem value="Java">

Upload a package to the package management service synchronously.

```java

  void upload(PackageMetadata metadata, String packageName, String path) throws PulsarAdminException;

```

Upload a package to the package management service asynchronously.

```java

  CompletableFuture<Void> uploadAsync(PackageMetadata metadata, String packageName, String path);

```

</TabItem>

</Tabs>
````

### Download a package

You can use the following commands to download a package.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages download function://public/default/example@v0.1 --path package-file

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version}

</TabItem>
<TabItem value="Java">

Download a package from the package management service synchronously.

```java

  void download(String packageName, String path) throws PulsarAdminException;

```

Download a package from the package management service asynchronously.

```java

  CompletableFuture<Void> downloadAsync(String packageName, String path);

```

</TabItem>

</Tabs>
````

### Delete a package

You can use the following commands to delete a package.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

The following command deletes a package of version 0.1.

```shell

bin/pulsar-admin packages delete functions://public/default/example@v0.1

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version}

</TabItem>
<TabItem value="Java">

Delete a specified package synchronously.

```java

  void delete(String packageName) throws PulsarAdminException;

```

Delete a specified package asynchronously.

```java

  CompletableFuture<Void> deleteAsync(String packageName);

```

</TabItem>

</Tabs>
````

### Get the metadata of a package

You can use the following commands to get the metadate of a package.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages get-metadata function://public/default/test@v1

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/metadata}

</TabItem>
<TabItem value="Java">

Get the metadata of a package synchronously.

```java

  PackageMetadata getMetadata(String packageName) throws PulsarAdminException;

```

Get the metadata of a package asynchronously.

```java

  CompletableFuture<PackageMetadata> getMetadataAsync(String packageName);

```

</TabItem>

</Tabs>
````

### Update the metadata of a package

You can use the following commands to update the metadata of a package.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages update-metadata function://public/default/example@v0.1 --description update-description

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/metadata}

</TabItem>
<TabItem value="Java">

Update the metadata of a package synchronously.

```java

  void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException;

```

Update the metadata of a package asynchronously.

```java

  CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata);

```

</TabItem>

</Tabs>
````

### List all versions of a package

You can use the following commands to list all versions of a package.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages list-versions type://tenant/namespace/packageName

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName}

</TabItem>
<TabItem value="Java">

List all versions of a package synchronously.

```java

  List<String> listPackageVersions(String packageName) throws PulsarAdminException;

```

List all versions of a package asynchronously.

```java

  CompletableFuture<List<String>> listPackageVersionsAsync(String packageName);

```

</TabItem>

</Tabs>
````

### List all packages of a specific type under a namespace

You can use the following commands to list all packages of a specific type under a namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>

<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages list --type function public/default

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v3/packages/:type/:tenant/:namespace}

</TabItem>
<TabItem value="Java">

List all packages of a specific type under a namespace synchronously.

```java

  List<String> listPackages(String type, String namespace) throws PulsarAdminException;

```

List all packages of a specific type under a namespace asynchronously.

```java

  CompletableFuture<List<String>> listPackagesAsync(String type, String namespace);

```

</TabItem>

</Tabs>
````
