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
package org.apache.pulsar.functions.runtime.worker.rest.api.v1;

import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;
import org.apache.pulsar.functions.runtime.worker.request.RequestResult;
import org.apache.pulsar.functions.runtime.worker.rest.RestUtils;
import org.apache.pulsar.functions.runtime.worker.rest.api.v1.dlog.DLOutputStream;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public final class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);


    private Utils(){}

    static boolean namespaceExists(String namespace, WorkerConfig workerConfig) {

        String zookeeperHost = workerConfig.getZookeeperUri().getHost();
        int zookeeperPort = workerConfig.getZookeeperUri().getPort();
        String destTopologyNamespaceURI = String.format("distributedlog://%s:%d/%s", zookeeperHost, zookeeperPort, namespace);

        URI uri = URI.create(destTopologyNamespaceURI);

        try {
            NamespaceBuilder.newBuilder().clientId("pulsar-functions-uploader").conf(getDlogConf(workerConfig)).uri(uri).build();
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    static String getDestPackageNamespaceURI(WorkerConfig workerConfig, String namespace) {
        String zookeeperHost = workerConfig.getZookeeperUri().getHost();
        int zookeeperPort = workerConfig.getZookeeperUri().getPort();
        return String.format("distributedlog://%s:%d/%s", zookeeperHost, zookeeperPort, namespace);
    }

    static URI getPackageURI(String destPackageNamespaceURI, String packageName) {
        return URI.create(String.format("%s/%s", destPackageNamespaceURI, packageName));
    }

    static URI uploadToBookeeper(InputStream uploadedInputStream,
                                 FormDataContentDisposition fileDetail,
                                 String namespace, WorkerConfig workerConfig)
            throws IOException {
        String packageName = fileDetail.getFileName();
        String destPackageNamespaceURI = getDestPackageNamespaceURI(workerConfig, namespace);
        URI packageURI = getPackageURI(destPackageNamespaceURI, packageName);

        DistributedLogConfiguration conf = getDlogConf(workerConfig);

        URI uri = URI.create(destPackageNamespaceURI);

        Namespace dlogNamespace = null;
        dlogNamespace = NamespaceBuilder.newBuilder()
                .clientId("pulsar-functions-uploader").conf(conf).uri(uri).build();

        // if the dest directory does not exist, create it.
        DistributedLogManager dlm = null;
        AppendOnlyStreamWriter writer = null;

        if (dlogNamespace.logExists(packageName)) {
            // if the destination file exists, write a log message
            LOG.info(String.format("Target function file already exists at '%s'. Overwriting it now",
                    packageURI.toString()));
            dlogNamespace.deleteLog(packageName);
        }
        // copy the topology package to target working directory
        LOG.info(String.format("Uploading function package '%s' to target DL at '%s'",
                fileDetail.getName(), packageURI.toString()));


        dlm = dlogNamespace.openLog(fileDetail.getFileName());
        writer = dlm.getAppendOnlyStreamWriter();

        try (OutputStream out = new DLOutputStream(dlm, writer)) {
            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = uploadedInputStream.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
            out.flush();
        }
        return packageURI;
    }

    static DistributedLogConfiguration getDlogConf(WorkerConfig workerConfig) {
        int numReplicas = workerConfig.getNumFunctionPackageReplicas();

        return new DistributedLogConfiguration()
                .setWriteLockEnabled(false)
                .setOutputBufferSize(256 * 1024)                  // 256k
                .setPeriodicFlushFrequencyMilliSeconds(0)         // disable periodical flush
                .setImmediateFlushEnabled(false)                  // disable immediate flush
                .setLogSegmentRollingIntervalMinutes(0)           // disable time-based rolling
                .setMaxLogSegmentBytes(Long.MAX_VALUE)            // disable size-based rolling
                .setExplicitTruncationByApplication(true)         // no auto-truncation
                .setRetentionPeriodHours(Integer.MAX_VALUE)       // long retention
                .setEnsembleSize(numReplicas)                     // replica settings
                .setWriteQuorumSize(numReplicas)
                .setAckQuorumSize(numReplicas)
                .setUseDaemonThread(true);
    }
}
