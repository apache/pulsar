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
package org.apache.pulsar.functions.runtime.worker;

import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.functions.runtime.worker.dlog.DLInputStream;
import org.apache.pulsar.functions.runtime.worker.dlog.DLOutputStream;

import java.io.*;
import java.net.URI;
import java.util.UUID;

@Slf4j
public final class Utils {

    private Utils(){}

    public static Object getObject(byte[] byteArr) throws IOException, ClassNotFoundException {
        Object obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(byteArr);
            ois = new ObjectInputStream(bis);
            obj = ois.readObject();
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (ois != null) {
                ois.close();
            }
        }
        return obj;
    }

    public static byte[] toByteArray(Object obj) throws IOException {
        byte[] bytes = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
        } finally {
            if (oos != null) {
                oos.close();
            }
            if (bos != null) {
                bos.close();
            }
        }
        return bytes;
    }

    public static boolean namespaceExists(String namespace, WorkerConfig workerConfig) {
        URI baseUri = URI.create(workerConfig.getDlogUri());
        String zookeeperHost = baseUri.getHost();
        int zookeeperPort = baseUri.getPort();
        String destTopologyNamespaceURI = String.format("distributedlog://%s:%d/%s", zookeeperHost, zookeeperPort, namespace);

        URI uri = URI.create(destTopologyNamespaceURI);

        try {
            NamespaceBuilder.newBuilder().clientId("pulsar-functions-uploader").conf(getDlogConf(workerConfig)).uri(uri).build();
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public static String getDestPackageNamespaceURI(WorkerConfig workerConfig, String namespace) {
        URI baseUri = URI.create(workerConfig.getDlogUri());
        String zookeeperHost = baseUri.getHost();
        int zookeeperPort = baseUri.getPort();
        return String.format("distributedlog://%s:%d/%s", zookeeperHost, zookeeperPort, namespace);
    }

    public static URI getPackageURI(String destPackageNamespaceURI, String packageName) {
        return URI.create(String.format("%s/%s-%s", destPackageNamespaceURI, packageName, UUID.randomUUID()));
    }

    public static String getUniquePackageName(String packageName) {
        return String.format("%s-%s", UUID.randomUUID().toString(), packageName);
    }

    public static void uploadToBookeeper(InputStream uploadedInputStream,
                                 FunctionMetaData functionMetaData, WorkerConfig workerConfig)
            throws IOException {

        String packageName = functionMetaData.getPackageLocation().getPackageName();
        String packageURI = functionMetaData.getPackageLocation().getPackageURI();
        DistributedLogConfiguration conf = getDlogConf(workerConfig);
        URI packageNamespaceURI = functionMetaData.getPackageLocation().getPackageNamespaceURI();

        Namespace dlogNamespace = NamespaceBuilder.newBuilder()
                .clientId("pulsar-functions-uploader").conf(conf).uri(packageNamespaceURI).build();

        // if the dest directory does not exist, create it.
        DistributedLogManager dlm = null;
        AppendOnlyStreamWriter writer = null;

        if (dlogNamespace.logExists(packageName)) {
            // if the destination file exists, write a log message
            log.info(String.format("Target function file already exists at '%s'. Overwriting it now",
                    packageURI));
            dlogNamespace.deleteLog(packageName);
        }
        // copy the topology package to target working directory
        log.info(String.format("Uploading function package '%s' to target DL at '%s'",
                packageName, packageURI));


        dlm = dlogNamespace.openLog(packageName);
        writer = dlm.getAppendOnlyStreamWriter();

        try (OutputStream out = new DLOutputStream(dlm, writer)) {
            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = uploadedInputStream.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
            out.flush();
        }
    }

    public static boolean downloadFromBookkeeper(URI uri, OutputStream outputStream, WorkerConfig workerConfig) {
        String path = uri.getPath();
        File pathFile = new File(path);
        String logName = pathFile.getName();
        String parentName = pathFile.getParent();
        DistributedLogConfiguration conf = getDlogConf(workerConfig);
        try {
            URI parentUri = new URI(
                    uri.getScheme(),
                    uri.getAuthority(),
                    parentName,
                    uri.getQuery(),
                    uri.getFragment());
            Namespace dlogNamespace = NamespaceBuilder.newBuilder()
                    .clientId("pulsar-functions-downloader").conf(conf).uri(parentUri).build();
            DistributedLogManager dlm = dlogNamespace.openLog(logName);
            InputStream in = new DLInputStream(dlm);
            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = in.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
            outputStream.flush();
            dlogNamespace.close();
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public static DistributedLogConfiguration getDlogConf(WorkerConfig workerConfig) {
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
