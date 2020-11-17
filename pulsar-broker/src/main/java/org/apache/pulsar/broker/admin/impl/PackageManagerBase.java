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
package org.apache.pulsar.broker.admin.impl;

import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.impl.PackageImpl;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class PackageManagerBase extends AdminResource {
    private static final Logger log = LoggerFactory.getLogger(PackageManagerBase.class);

    protected PackageImpl getPackageManager() {
        return pulsar().getPackageManagerService();
    }

    protected PackageMetadata internalGetMeta(String type, String tenant, String namespace, String pacakgeName, String version) {
        try {
            PackageName name = PackageName.get(type, tenant, namespace, pacakgeName, version);
            return getPackageManager().getMeta(name).get();
        } catch (IllegalArgumentException illegalArgumentException) {
            throw new RestException(Response.Status.PRECONDITION_FAILED, illegalArgumentException.getMessage());
        } catch (InterruptedException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (ExecutionException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        }
    }

    protected void internalUploadMeta(String type, String tenant, String namespace, String packageName, String version, PackageMetadata metadata) {
        try {
            PackageName name = PackageName.get(type, tenant, namespace, packageName, version);
            System.out.println("pacakge name is " + name.toString());
            getPackageManager().setMeta(name, metadata).get();
        } catch (IllegalArgumentException illegalArgumentException) {
            illegalArgumentException.printStackTrace();
            throw new RestException(Response.Status.PRECONDITION_FAILED, illegalArgumentException.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        }
    }

    protected void internalUpdateMeta(String type, String tenant, String namespace, String packageName, String version, PackageMetadata metadata) {
        try {
            PackageName name = PackageName.get(type, tenant, namespace, packageName, version);
            getPackageManager().updateMeta(name, metadata).get();
        } catch (IllegalArgumentException illegalArgumentException) {
            throw new RestException(Response.Status.PRECONDITION_FAILED, illegalArgumentException.getMessage());
        } catch (InterruptedException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (ExecutionException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        }
    }

    protected StreamingOutput internalDownload(String type, String tenant, String namespace, String packageName, String version) {
        try {
            PackageName name = PackageName.get(type, tenant, namespace, packageName, version);
            return output -> {
                try {
                    getPackageManager().download(name, output).get();
                } catch (InterruptedException e) {
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                } catch (ExecutionException e) {
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
                }
            };
        } catch (IllegalArgumentException illegalArgumentException) {
            throw new RestException(Response.Status.PRECONDITION_FAILED, illegalArgumentException.getMessage());
        }
    }

    protected void internalUpload(String type, String tenant, String namespace, String pacakgeName, String version, PackageMetadata metadata, InputStream uploadedInputStream) {
        try {
            PackageName name = PackageName.get(type, tenant, namespace, pacakgeName, version);
            getPackageManager().upload(name, metadata, uploadedInputStream).get();
        } catch (IllegalArgumentException iae) {
            throw new RestException(Response.Status.PRECONDITION_FAILED, iae.getMessage());
        }catch (InterruptedException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (ExecutionException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        }
    }

    protected void internalDelete(String type, String tenant, String namespace, String pacakgeName, String version) {
        try {
            PackageName name = PackageName.get(type, tenant, namespace, pacakgeName, version);
            getPackageManager().delete(name).get();
        } catch (IllegalArgumentException iae) {
            throw new RestException(Response.Status.PRECONDITION_FAILED, iae.getMessage());
        }catch (InterruptedException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (ExecutionException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        }
    }
    protected List<PackageName> internalList(String type, String tenant, String namespace, String pacakgeName) {
        try {
            PackageName name = PackageName.get(type, tenant, namespace, pacakgeName, "");
            return getPackageManager().list(name).get();
        } catch (IllegalArgumentException iae) {
            throw new RestException(Response.Status.PRECONDITION_FAILED, iae.getMessage());
        }catch (InterruptedException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (ExecutionException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        }
    }

    protected List<PackageName> internalList(String type, String tenant, String namespace) {
        try {
            PackageType packageType = PackageType.getEnum(type);
            NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
            return getPackageManager().list(packageType, namespaceName).get();
        } catch (IllegalArgumentException iae) {
            throw new RestException(Response.Status.PRECONDITION_FAILED, iae.getMessage());
        }catch (InterruptedException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (ExecutionException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        }
    }
}
