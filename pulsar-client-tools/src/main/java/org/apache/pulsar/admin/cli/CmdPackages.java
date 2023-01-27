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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;

/**
 * Commands for administering packages.
 */
@Parameters(commandDescription = "Operations about packages")
class CmdPackages extends CmdBase {

    private Packages packages;

    public CmdPackages(Supplier<PulsarAdmin> admin) {
        super("packages", admin);


        jcommander.addCommand("get-metadata", new GetMetadataCmd());
        jcommander.addCommand("update-metadata", new UpdateMetadataCmd());
        jcommander.addCommand("upload", new UploadCmd());
        jcommander.addCommand("download", new DownloadCmd());
        jcommander.addCommand("list", new ListPackagesCmd());
        jcommander.addCommand("list-versions", new ListPackageVersionsCmd());
        jcommander.addCommand("delete", new DeletePackageCmd());
    }

    private Packages getPackages() {
        if (packages == null) {
            packages = getAdmin().packages();
        }
        return packages;
    }

    @Parameters(commandDescription = "Get a package metadata information.")
    private class GetMetadataCmd extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Override
        void run() throws Exception {
            print(getPackages().getMetadata(packageName));
        }
    }

    @Parameters(commandDescription = "Update a package metadata information.")
    private class UpdateMetadataCmd extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = {"-d", "--description"}, description = "descriptions of a package", required = true)
        private String description;

        @Parameter(names = {"-c", "--contact"}, description = "contact info of a package")
        private String contact;

        @DynamicParameter(names = {"--properties", "-P"},  description = "external information of a package")
        private Map<String, String> properties = new HashMap<>();

        @Override
        void run() throws Exception {
            getPackages().updateMetadata(packageName, PackageMetadata.builder()
                .description(description).contact(contact).properties(properties).build());
            print(String.format("The metadata of the package '%s' updated successfully", packageName));
        }
    }

    @Parameters(commandDescription = "Upload a package")
    private class UploadCmd extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--description", description = "descriptions of a package", required = true)
        private String description;

        @Parameter(names = "--contact", description = "contact information of a package")
        private String contact;

        @DynamicParameter(names = {"--properties", "-P"}, description = "external information of a package")
        private Map<String, String> properties = new HashMap<>();

        @Parameter(names = "--path", description = "file path of the package", required = true)
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

    @Parameters(commandDescription = "Download a package")
    private class DownloadCmd extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--path", description = "download destiny path of the package", required = true)
        private String path;

        @Override
        void run() throws Exception {
            getPackages().download(packageName, path);
            print(String.format("The package '%s' downloaded to path '%s' successfully", packageName, path));
        }
    }

    @Parameters(commandDescription = "List all versions of the given package")
    private class ListPackageVersionsCmd extends CliCommand {
        @Parameter(description = "the package name you want to query, don't need to specify the package version. "
                + "type://tenant/namespace/packageName", required = true)
        private String packageName;

        @Override
        void run() throws Exception {
            print(getPackages().listPackageVersions(packageName));
        }
    }

    @Parameters(commandDescription = "List all packages with given type in the specified namespace")
    private class ListPackagesCmd extends CliCommand {
        @Parameter(names = "--type", description = "type of the package", required = true)
        private String type;

        @Parameter(description = "namespace of the package", required = true)
        private String namespace;

        @Override
        void run() throws Exception {
            print(getPackages().listPackages(type, namespace));
        }
    }

    @Parameters(commandDescription = "Delete a package")
    private class DeletePackageCmd extends CliCommand{
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Override
        void run() throws Exception {
            getPackages().delete(packageName);
            print(String.format("The package '%s' deleted successfully", packageName));
        }
    }
}
