/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.admin.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Commands for administering packages.
 */
@Command(description = "Operations about packages")
class CmdPackages extends CmdBase {

    private Packages packages;

    public CmdPackages(Supplier<PulsarAdmin> admin) {
        super("packages", admin);


        addCommand("get-metadata", new GetMetadataCmd());
        addCommand("update-metadata", new UpdateMetadataCmd());
        addCommand("upload", new UploadCmd());
        addCommand("download", new DownloadCmd());
        addCommand("list", new ListPackagesCmd());
        addCommand("list-versions", new ListPackageVersionsCmd());
        addCommand("delete", new DeletePackageCmd());
    }

    private Packages getPackages() {
        if (packages == null) {
            packages = getAdmin().packages();
        }
        return packages;
    }

    @Command(description = "Get a package metadata information.")
    private class GetMetadataCmd extends CliCommand {
        @Parameters(description = "type://tenant/namespace/packageName@version", arity = "1")
        private String packageName;

        @Override
        void run() throws Exception {
            print(getPackages().getMetadata(packageName));
        }
    }

    @Command(description = "Update a package metadata information.")
    private class UpdateMetadataCmd extends CliCommand {
        @Parameters(description = "type://tenant/namespace/packageName@version", arity = "1")
        private String packageName;

        @Option(names = {"-d", "--description"}, description = "descriptions of a package", required = true)
        private String description;

        @Option(names = {"-c", "--contact"}, description = "contact info of a package")
        private String contact;

        @Option(names = {"--properties", "-P"}, description = "external information of a package")
        private Map<String, String> properties = new HashMap<>();

        @Override
        void run() throws Exception {
            getPackages().updateMetadata(packageName, PackageMetadata.builder()
                .description(description).contact(contact).properties(properties).build());
            print(String.format("The metadata of the package '%s' updated successfully", packageName));
        }
    }

    @Command(description = "Upload a package")
    private class UploadCmd extends CliCommand {
        @Parameters(description = "type://tenant/namespace/packageName@version", arity = "1")
        private String packageName;

        @Option(names = "--description", description = "descriptions of a package", required = true)
        private String description;

        @Option(names = "--contact", description = "contact information of a package")
        private String contact;

        @Option(names = {"--properties", "-P"}, description = "external information of a package")
        private Map<String, String> properties = new HashMap<>();

        @Option(names = "--path", description = "file path of the package", required = true)
        private String path;

        @Override
        void run() throws Exception {
            PackageMetadata metadata = PackageMetadata.builder()
                .description(description)
                .contact(contact)
                .properties(properties).build();
            getPackages().upload(metadata, packageName, path);
            print(String.format("The package '%s' uploaded from path '%s' successfully", packageName, path));
        }
    }

    @Command(description = "Download a package")
    private class DownloadCmd extends CliCommand {
        @Parameters(description = "type://tenant/namespace/packageName@version", arity = "1")
        private String packageName;

        @Option(names = "--path", description = "download destiny path of the package", required = true)
        private String path;

        @Override
        void run() throws Exception {
            getPackages().download(packageName, path);
            print(String.format("The package '%s' downloaded to path '%s' successfully", packageName, path));
        }
    }

    @Command(description = "List all versions of the given package")
    private class ListPackageVersionsCmd extends CliCommand {
        @Parameters(description = "the package name you want to query, don't need to specify the package version. "
                + "type://tenant/namespace/packageName", arity = "1")
        private String packageName;

        @Override
        void run() throws Exception {
            print(getPackages().listPackageVersions(packageName));
        }
    }

    @Command(description = "List all packages with given type in the specified namespace")
    private class ListPackagesCmd extends CliCommand {
        @Option(names = "--type", description = "type of the package", required = true)
        private String type;

        @Parameters(description = "namespace of the package", arity = "1")
        private String namespace;

        @Override
        void run() throws Exception {
            print(getPackages().listPackages(type, namespace));
        }
    }

    @Command(description = "Delete a package")
    private class DeletePackageCmd extends CliCommand {
        @Parameters(description = "type://tenant/namespace/packageName@version", arity = "1")
        private String packageName;

        @Override
        void run() throws Exception {
            getPackages().delete(packageName);
            print(String.format("The package '%s' deleted successfully", packageName));
        }
    }
}
