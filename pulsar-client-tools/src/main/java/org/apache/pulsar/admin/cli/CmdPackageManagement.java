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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import org.apache.pulsar.client.admin.PackageManagement;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.packages.manager.PackageMetadata;

import java.util.HashMap;
import java.util.Map;

@Parameters(commandDescription = "Operations on package management")
public class CmdPackageManagement extends CmdBase{
    private PackageManagement packageManagement;

    public CmdPackageManagement(PulsarAdmin admin) {
        super("package", admin);
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
            print(packageName);

            print(packageManagement.getMetadata(packageName));
        }
    }

    @Parameters(commandDescription = "Update a package metadata information.")
    private class UpdateMetadata extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--description", description=  "descriptions of a package", required = true)
        private String description;

        @Parameter(names = "--contact", description = "contact info of a package")
        private String contact;

        @DynamicParameter(names = {"--properties", "-P"},  description ="external information of a package")
        private Map<String, String> properties = new HashMap<>();

        @Override
        void run() throws Exception {
            print(packageName);
            print(description);
            print(contact);
            print(new Gson().toJson(properties));
            packageManagement.updateMetadata(packageName, PackageMetadata.builder()
                .description(description).contact(contact).properties(properties).build());
        }
    }

    @Parameters(commandDescription = "Upload a package")
    private class Upload extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--description", description=  "descriptions of a package", required = true)
        private String description;

        @Parameter(names = "--contact", description = "contact information of a package")
        private String contact;

        @DynamicParameter(names = {"--properties", "-P"},  description ="external infromations of a package")
        private Map<String, String> properties = new HashMap<>();

        @Parameter(names = "--path", description = "descriptions of a package", required = true)
        private String path;

        @Override
        void run() throws Exception {
            print(packageName);
            print(description);
            print(contact);
            print(new Gson().toJson(properties));
            print(path);
            PackageMetadata metadata = PackageMetadata.builder()
                .description(description)
                .contact(contact)
                .properties(properties).build();
            packageManagement.upload(metadata, packageName, path);
        }
    }

    @Parameters(commandDescription = "Download a package")
    private class Download extends CliCommand {
        @Parameter(description = "type://tenant/namespace/packageName@version", required = true)
        private String packageName;

        @Parameter(names = "--path", description = "download destiny path of the package", required = true)
        private String path;

        @Override
        void run() throws Exception {
            print(path);
            packageManagement.download(packageName, path);
        }
    }

    @Parameters(commandDescription = "List all versions of the given package")
    private class ListPackageVersions extends CliCommand {
        @Parameter(description = "the package name you want to query, don't need to specify the package version.\n" +
            "type://tenant/namespace/packageName", required = true)
        private String packageName;

        @Override
        void run() throws Exception {
            print(packageName);
            print(packageManagement.listPackageVersions(packageName).toString());
        }
    }

    @Parameters(commandDescription = "List all packages with given type in the specified namespace")
    private class ListPackages extends CliCommand {
        @Parameter(names = "--type", description = "type of the package", required = true)
        private String type;

        @Parameter(description = "namespace of the package", required = true)
        private String namespace;

        @Override
        void run() throws Exception {
            print(type);
            print(namespace);
            print(packageManagement.listPackages(type, namespace));
        }
    }


}
