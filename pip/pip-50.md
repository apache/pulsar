# PIP-50: Package Management

* **Status**: Drafting
 * **Author**: Yong Zhang
 * **Pull Request**: 
 * **Mailing List discussion**:

# Motivation

This document proposes a new way to manage the packages used in Functions, Sinks, and Sources. Package management is used to improve the management of user functions and connectors at Apache Pulsar. I will use \`package\` to refer to user function executables and connectors in the remainder of the proposal.

Apache Pulsar provides Functions for users transforming their data as they want.Functions is a popular tool Pulsar users leverage to transform their data. While it offers many benefits, we have identified a number of ways to improve the user experience. For example, today,  If there are many users who use the same function in different namespaces, they need to upload the same function multiple times. The same problem exists in Sinks and Sources as well. 

Also, there is no version management in Functions, Sinks, and Sources. If a new version of a Function is developed, users have to update the function with a new package. If bugs are found in the new version of the Function, users have to rollback the function by updating the function with an old package. That means users have to manage their packages in their own services. 

Package Management can provide an easier way to manage the packages and simplify function upgrade and rollback processes. The proposal extends the existing package upload and download capabilities of Pulsar to a common package management system.

# Implementation

### Package Name

We introduce a new resource \`package\` in Pulsar. A package is identified by five parts \- \`type\`, \`tenant\`, \`namespace\`, \`package name\`, and \`version\`. 

\`\`\`  
class PackageName {  
    private final PackageType type;  
    // REST API handlers.  
    private final String namespace;  
    private final String tenant;  
    private final String name;  
    private final String version;  
}

enum PackageType {  
    FUNCTION("function"), SINK("sink"), SOURCE("source");  
}

\`\`\`

\`type\`: indicates the type of the package. The initial supported types are: \`function\`, \`sink\` and \`source\`. 

\`name\`: indicates the fully qualified name of the package \- \`\<tenant\>/\<namespace\>/\<package name\>\`

\`version\`: indicates the version of the package.

\---

A package can be located using a URL. The package URL is written in the following format:

\`\`\`  
\<type\>://\<tenant\>/\<namespace\>/\<package name\>@\<version\>  
\`\`\`

Example package URLs are listed as follows:

- \`sink://public/default/mysql-sink@1.0\`  
- \`function://my-tenant/my-ns/my-function@0.1\`  
- \`source://my-tenant/my-ns/mysql-cdc-source@2.3\`

Besides storing the data and versions of a package, the package management system also stores additional metadata for each package (version).

The proposal metadata is defined as below:

- ‘description’: the description of the package.  
- ‘contact’: the contact information of a package. For example, team email  
- ‘create\_time’: the creation time of the package.  
- ‘modification\_time’: the modification time of the package.  
- ‘properties’: a key/value map that allows users to store their own information.

### Restful Endpoint

We will introduce a new restful endpoint called \`packages\` for handling the management of packages. This \`packages\` endpoint will support the following operations:

#### GET /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>/\<pacakage-name\>/\<version\>/metadata

Get the metadata of a package

#### PUT /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>/\<pacakage-name\>/\<version\>/metadata

Update the metadata of a package

#### GET /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>/\<package-namepacakge-name\>/\<version\>

Download a package of a given version

#### POST /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>/\<package-namepacakge-name\>/\<version\>

Upload a package of a given version

#### DELETE /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>/\<pacakge-name\>/\<version\>

Delete a package of a given version

#### DELETE /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>/\<pacakge-name\>

Delete all the versions of a package

#### GET /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>/\<pacakge-name\>

List all the versions of a package

#### GET /admin/v3/packages/\<type\>/\<tenant\>/\<namespace\>

List all the packages of a namespace

### Client 

#### Java Client

At the java client, we will introduce a new API \`Packages\`.

\`\`\`  
*/\*\**  
 *\* Administration operations of the packages management service.*  
 *\*/*  
public interface Packages {  
    */\*\**  
     *\* Get a package metadata information.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name of the package metadata you want to find*  
     *\* @return the package metadata information*  
     *\*/*  
    PackageMetadata getMetadata(String packageName) throws PulsarAdminException;

    */\*\**  
     *\* Get a package metadata information asynchronously.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name of the package metadata you want to find*  
     *\* @return  the package metadata information*  
     *\*/*  
    CompletableFuture\<PackageMetadata\> getMetadataAsync(String packageName);

    */\*\**  
     *\* Update a package metadata information.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name of the package metadata you want to update*  
     *\* @param metadata*  
     *\*          the updated metadata information*  
     *\*/*  
    void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException;

    */\*\**  
     *\* Update a package metadata information asynchronously.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name of the package metadata you want to update*  
     *\* @param metadata*  
     *\*          the updated metadata information*  
     *\* @return nothing*  
     *\*/*  
    CompletableFuture\<Void\> updateMetadataAsync(String packageName, PackageMetadata metadata);

    */\*\**  
     *\* Upload a package to the package management service.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name of the upload file*  
     *\* @param path*  
     *\*          the upload file path*  
     *\*/*  
    void upload(PackageMetadata metadata, String packageName, String path) throws PulsarAdminException;

    */\*\**  
     *\* Upload a package to the package management service asynchronously.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name you want to upload*  
     *\* @param path*  
     *\*          the path you want to upload from*  
     *\* @return nothing*  
     *\*/*  
    CompletableFuture\<Void\> uploadAsync(PackageMetadata metadata, String packageName, String path);

    */\*\**  
     *\* Download a package from the package management service.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name you want to download*  
     *\* @param path*  
     *\*          the path you want to download to*  
     *\*/*  
    void download(String packageName, String path) throws PulsarAdminException;

    */\*\**  
     *\* Download a package from the package management service asynchronously.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name you want to download*  
     *\* @param path*  
     *\*          the path you want to download to*  
     *\* @return nothing*  
     *\*/*  
    *CompletableFuture\<Void\> downloadAsync(String packageName, String path);*

    */\*\**  
     *\* Delete the specified package.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name which you want to delete*  
     *\*/*  
    void delete(String packageName) throws PulsarAdminException;

    */\*\**  
     *\* Delete the specified package asynchronously.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name which you want to delete*  
     *\* @return nothing*  
     *\*/*  
    CompletableFuture\<Void\> deleteAsync(String packageName);

    */\*\**  
     *\* List all the versions of a package.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name which you want to get all the versions*  
     *\* @return all the versions of the package*  
     *\*/*  
    List\<String\> listPackageVersions(String packageName) throws PulsarAdminException;

    */\*\**  
     *\* List all the versions of a package asynchronously.*  
     *\**  
     *\* @param packageName*  
     *\*          the package name which you want to get all the versions*  
     *\* @return all the versions of the package*  
     *\*/*  
    CompletableFuture\<List\<String\>\> listPackageVersionsAsync(String packageName);

    */\*\**  
     *\* List all the packages with the given type in a namespace.*  
     *\**  
     *\* @param type*  
     *\*          the type you want to get the packages*  
     *\* @param namespace*  
     *\*          the namespace you want to get the packages*  
     *\* @return all the packages of the given type which in the given namespace*  
     *\*/*

    List\<String\> listPackages(String type, String namespace) throws PulsarAdminException;  
    */\*\**  
     *\* List all the packages with the given type in a namespace asynchronously.*  
     *\**  
     *\* @param type*  
     *\*          the type you want to get the packages*  
     *\* @param namespace*  
     *\*          the namespace you want to get the packages*  
     *\* @return all the packages of the given type which in the given namespace*  
     *\*/*  
    CompletableFuture\<List\<String\>\> listPackagesAsync(String type, String namespace);  
*}*

\`\`\`

These operations will create an HTTP request like the other command in the admin and handle it by the REST APIs on the server-side.

#### Admin CLI

At the command line tools, we will introduce a new command group  \`packages\`. Users can manage the packages using this command group.  

Examples:

\`\`\`  
// Upload a package  
bin/pulsar-admin packages upload functions://public/default/example@v0.1 \--path package-file \--description package-description

// Download a package  
bin/pulsar-admin packages download functions://public/default/example@v0.1 \--path package-file

// Delete a package of version 0.1  
bin/pulsar-admin packages delete functions://public/default/example@v0.1

// Delete all the versions of a package  
bin/pulsar-admin packages delete functions://public/default/example

// List all the specified type packages under a namespace  
bin/pulsar-admin packages list \--type function public/default  
\`\`\`

### Permissions

Because the packages are organisedorganized by the tenant and namespace. We can use the existing mechanism to scope the packages. The tenant and namespace permissions can apply to packages directly.

### Changes in Functions Runtime

Pulsar Functions supports three ways to run a function, thread, process, and Kubernetes. 

In thread or process mode, a package file will download from the BookKeeper if the package is not provided with a URL. We need to add a new method \`WorkerUtils.isPackageURL\` to check and then download it with the new package URL.

If Pulsar Functions managed by Kubernetes, a package file will download using pulsar-admin command-line tools. So we need to update the \`pulsar-admin functions download\` command to support download with the new package URL.  
