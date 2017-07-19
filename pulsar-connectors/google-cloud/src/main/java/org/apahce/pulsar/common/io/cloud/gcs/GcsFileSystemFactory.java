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
package org.apahce.pulsar.common.io.cloud.gcs;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.auto.service.AutoService;

import org.apache.pulsar.common.io.FileSystem;
import org.apache.pulsar.common.io.FileSystemFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Properties;

@AutoService(FileSystemFactory.class)
public class GcsFileSystemFactory implements FileSystemFactory {

    private static final String KEY_SERVICE_ACCOUNT_KEY_FILE =
            "google.cloud.service.account.keyfile";

    @Override
    public FileSystem create(Properties properties) {
        final Storage storage;
        try {
            storage = createStorage(properties);
        } catch (IOException | GeneralSecurityException ex) {
            throw new RuntimeException(ex);
        }

        return new GcsFileSystem(GcsHelper.create(storage));
    }

    private Storage createStorage(Properties properties)
            throws IOException, GeneralSecurityException {
        final Credential credential = createCredential(properties);
        final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        return new Storage.Builder(httpTransport, jsonFactory, credential).build();
    }

    private Credential createCredential(Properties properties) throws IOException {
        if (properties.containsKey(KEY_SERVICE_ACCOUNT_KEY_FILE)) {
            final String credentialsPath =
                    properties.getProperty(KEY_SERVICE_ACCOUNT_KEY_FILE);
            return GoogleCredential
                    .fromStream(new FileInputStream(credentialsPath))
                    .createScoped(StorageScopes.all());
        }

        return GoogleCredential.getApplicationDefault().createScoped(StorageScopes.all());
    }
}
