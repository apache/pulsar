---
id: admin-api-packages
title: Manage packages
sidebar_label: "Packages"
original_id: admin-api-packages
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Package management enables version management and simplifies the upgrade and rollback processes for Functions, Sinks, and Sources. When you use the same function, sink and source in different namespaces, you can upload them to a common package management system.

## Package name

A `package` is identified by five parts: `type`, `tenant`, `namespace`, `package name`, and `version`.

| Part  | Description |
|-------|-------------|
|`type` |The type of the package. The following types are supported: `function`, `sink` and `source`. |
| `name`|The fully qualified name of the package: `<tenant>/<namespace>/<package name>`.|
|`version`|The version of the package.|

The following is a code sample.

```java

class PackageName {
   private final PackageType type;
   private final String namespace;
   private final String tenant;
   private final String name;
   private final String version;
}

enum PackageType {
   FUNCTION("function"), SINK("sink"), SOURCE("source");
}

```

## Package URL
A package is located using a URL. The package URL is written in the following format:

```shell

<type>://<tenant>/<namespace>/<package name>@<version>

```

The following are package URL examples:

`sink://public/default/mysql-sink@1.0`   
`function://my-tenant/my-ns/my-function@0.1`   
`source://my-tenant/my-ns/mysql-cdc-source@2.3`

The package management system stores the data, versions and metadata of each package. The metadata is shown in the following table.

| metadata | Description |
|----------|-------------|
|description|The description of the package.|
|contact    |The contact information of a package. For example, team email.|
|create_time| The time when the package is created.|
|modification_time| The time when the package is modified.|
|properties |A key/value map that stores your own information.|

## Permissions

The packages are organized by the tenant and namespace, so you can apply the tenant and namespace permissions to packages directly.

## Package resources
You can use the package management with command line tools, REST API and Java client.

### Upload a package
You can upload a package to the package management service in the following ways.

<Tabs 
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"JAVA","value":"JAVA"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages upload function://public/default/example@v0.1 --path package-file --description package-description

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

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

### Download a package
You can download a package to the package management service in the following ways.

<Tabs 
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"JAVA","value":"JAVA"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages download function://public/default/example@v0.1 --path package-file

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

Download a package to the package management service synchronously.

```java

   void download(String packageName, String path) throws PulsarAdminException;

```

Download a package to the package management service asynchronously.

```java

   CompletableFuture<Void> downloadAsync(String packageName, String path);

```

</TabItem>

</Tabs>

### List all versions of a package
You can get a list of all versions of a package in the following ways.
<Tabs 
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"JAVA","value":"JAVA"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages list --type function public/default

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

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

### List all the specified type packages under a namespace
You can get a list of all the packages with the given type in a namespace in the following ways.
<Tabs 
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"JAVA","value":"JAVA"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages list --type function public/default

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v3/packages/:type/:tenant/:namespace/?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

List all the packages with the given type in a namespace synchronously.

```java

   List<String> listPackages(String type, String namespace) throws PulsarAdminException;

```

List all the packages with the given type in a namespace asynchronously.

```java

   CompletableFuture<List<String>> listPackagesAsync(String type, String namespace);

```

</TabItem>

</Tabs>

### Get the metadata of a package
You can get the metadata of a package in the following ways.

<Tabs 
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"JAVA","value":"JAVA"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages get-metadata function://public/default/test@v1

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/metadata/?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

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

### Update the metadata of a package
You can update the metadata of a package in the following ways.
<Tabs 
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"JAVA","value":"JAVA"}]}>
<TabItem value="pulsar-admin">

```shell

bin/pulsar-admin packages update-metadata function://public/default/example@v0.1 --description update-description

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/metadata/?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

Update a package metadata information synchronously.

```java

   void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException;

```

Update a package metadata information asynchronously.

```java

   CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata);

```

</TabItem>

</Tabs>

### Delete a specified package
You can delete a specified package with its package name in the following ways.

<Tabs 
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"JAVA","value":"JAVA"}]}>
<TabItem value="pulsar-admin">

The following command example deletes a package of version 0.1.

```shell

bin/pulsar-admin packages delete function://public/default/example@v0.1

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

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
