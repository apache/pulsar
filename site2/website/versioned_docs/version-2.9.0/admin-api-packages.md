---
id: version-2.9.0-admin-api-packages
title: Manage packages
sidebar_label: Packages
original_id: admin-api-packages
---

> **Important**
>
> This page only shows **some frequently used operations**.
>
> - For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](https://pulsar.apache.org/tools/pulsar-admin/)
> 
> - For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.
> 
> - For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](https://pulsar.apache.org/api/admin/).

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

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
bin/pulsar-admin packages upload functions://public/default/example@v0.1 --path package-file --description package-description
```

<!--REST API-->

{@inject: endpoint|POST|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/?version=[[pulsar:version_number]]}

<!--JAVA-->
Upload a package to the package management service synchronously.

```java
   void upload(PackageMetadata metadata, String packageName, String path) throws PulsarAdminException;
```
Upload a package to the package management service asynchronously.
```java
   CompletableFuture<Void> uploadAsync(PackageMetadata metadata, String packageName, String path);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Download a package
You can download a package to the package management service in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
bin/pulsar-admin packages download functions://public/default/example@v0.1 --path package-file
```

<!--REST API-->

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/?version=[[pulsar:version_number]]}

<!--JAVA-->
Download a package to the package management service synchronously.

```java
   void download(String packageName, String path) throws PulsarAdminException;
```

Download a package to the package management service asynchronously.
```java
   CompletableFuture<Void> downloadAsync(String packageName, String path);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### List all versions of a package
You can get a list of all versions of a package in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
bin/pulsar-admin packages list --type function public/default
```

<!--REST API-->

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/?version=[[pulsar:version_number]]}

<!--JAVA-->
List all versions of a package synchronously.

```java
   List<String> listPackageVersions(String packageName) throws PulsarAdminException;
```
List all versions of a package asynchronously.
```java
   CompletableFuture<List<String>> listPackageVersionsAsync(String packageName);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### List all the specified type packages under a namespace
You can get a list of all the packages with the given type in a namespace in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
bin/pulsar-admin packages list --type function public/default
```

<!--REST API-->

{@inject: endpoint|PUT|/admin/v3/packages/:type/:tenant/:namespace/?version=[[pulsar:version_number]]}

<!--JAVA-->
List all the packages with the given type in a namespace synchronously.

```java
   List<String> listPackages(String type, String namespace) throws PulsarAdminException;
```
List all the packages with the given type in a namespace asynchronously.
```java
   CompletableFuture<List<String>> listPackagesAsync(String type, String namespace);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Get the metadata of a package
You can get the metadata of a package in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```shell
bin/pulsar-admin packages get-metadata function://public/default/test@v1
```

<!--REST API-->

{@inject: endpoint|GET|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/metadata/?version=[[pulsar:version_number]]}

<!--JAVA-->
Get the metadata of a package synchronously.

```java
   PackageMetadata getMetadata(String packageName) throws PulsarAdminException;
```
Get the metadata of a package asynchronously.
```java
   CompletableFuture<PackageMetadata> getMetadataAsync(String packageName);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Update the metadata of a package
You can update the metadata of a package in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
bin/pulsar-admin packages update-metadata function://public/default/example@v0.1 --description update-description
```

<!--REST API-->

{@inject: endpoint|PUT|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/metadata/?version=[[pulsar:version_number]]}

<!--JAVA-->
Update a package metadata information synchronously.

```java
   void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException;
```
Update a package metadata information asynchronously.
```java
   CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Delete a specified package
You can delete a specified package with its package name in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
The following command example deletes a package of version 0.1.

```shell
bin/pulsar-admin packages delete functions://public/default/example@v0.1
```

<!--REST API-->

{@inject: endpoint|DELETE|/admin/v3/packages/:type/:tenant/:namespace/:packageName/:version/?version=[[pulsar:version_number]]}

<!--JAVA-->
Delete a specified package synchronously.

```java
   void delete(String packageName) throws PulsarAdminException;
```
Delete a specified package asynchronously.
```java
   CompletableFuture<Void> deleteAsync(String packageName);
```
<!--END_DOCUSAURUS_CODE_TABS-->
