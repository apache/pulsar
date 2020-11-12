/**
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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.pulsar.client.admin.PackageManagement;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.packages.manager.PackageMetadata;

@Parameters(commandDescription = "Operations on package management")
public class CmdPackageManagement extends CmdBase{
    private PackageManagement packageManagement;

    public CmdPackageManagement(PulsarAdmin admin) {
        super("packages", admin);
        this.packageManagement = admin.packageManagement();

        jcommander.addCommand("metadata", new GetMetadataCmd());
        jcommander.addCommand("update-metadata", new UpdateMetadata());
        jcommander.addCommand("upload", new Upload());
        jcommander.addCommand("download", new Download());
        jcommander.addCommand("list-versions", new ListPackageVersions());
        jcommander.addCommand("list", new ListPackages());
    }

    @Parameters(commandDescription = "Get a package metadata information.")
    private class GetMetadataCmd extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Override
        void run() throws Exception {
            print(packageManagement.getMetadata(packageName).toString());
        }
    }

    @Parameters(commandDescription = "Update a package metadata information.")
    private class UpdateMetadata extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--description", description=  "descriptions of a package", required = true)
        private String description;

        @Parameter(names = "--contact", description = "")
        private String contact;

        @Override
        void run() throws Exception {
            packageManagement.updateMetadata(packageName,
                PackageMetadata.builder().description(description).contact(contact).build());
        }
    }
    @Parameters(commandDescription = "Upload a package")
    private class Upload extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--path", description = "descriptions of a package", required = true)
        private String path;

        @Override
        void run() throws Exception {
            packageManagement.upload(packageName, path);
        }
    }

    @Parameters(commandDescription = "Download a package")
    private class Download extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--path", description = "descriptions of a package", required = true)
        private String path;

        @Override
        void run() throws Exception {
            packageManagement.download(packageName, path);
        }
    }

    @Parameters(commandDescription = "List all versions of the given package")
    private class ListPackageVersions extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Override
        void run() throws Exception {
            print(packageManagement.listPackageVersions(packageName).toString());
        }
    }

    @Parameters(commandDescription = "List all packages with given type in the specified namespace")
    private class ListPackages extends CliCommand {
        @Parameter(description = "type of the package")
        private String type;
        @Parameter(description = "namespace of the package")
        private String namespace;

        @Override
        void run() throws Exception {
            print(packageManagement.listPackages(type, namespace));
        }
    }


}
