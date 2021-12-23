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
package org.apache.pulsar.io.hdfs2;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.SocketFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pulsar.io.hdfs2.sink.HdfsSinkConfig;

/**
 * A Simple abstract class for HDFS connectors.
 * Provides methods for connecting to HDFS
 */
public abstract class AbstractHdfsConnector {

    private static final Object RESOURCES_LOCK = new Object();

    // Hadoop Configuration, Filesystem, and UserGroupInformation (optional)
    protected final AtomicReference<HdfsResources> hdfsResources = new AtomicReference<>();
    protected AbstractHdfsConfig connectorConfig;
    protected CompressionCodecFactory compressionCodecFactory;

    public AbstractHdfsConnector() {
       hdfsResources.set(new HdfsResources(null, null, null));
    }

    /*
     * Reset Hadoop Configuration and FileSystem based on the supplied configuration resources.
     */
    protected HdfsResources resetHDFSResources(HdfsSinkConfig hdfsSinkConfig) throws IOException {
        Configuration config = new ExtendedConfiguration();
        config.setClassLoader(Thread.currentThread().getContextClassLoader());

        getConfig(config, connectorConfig.getHdfsConfigResources());

        // first check for timeout on HDFS connection, because FileSystem has a hard coded 15 minute timeout
        checkHdfsUriForTimeout(config);

        /* Disable caching of Configuration and FileSystem objects, else we cannot reconfigure
         * the processor without a complete restart
         */
        String disableCacheName = String.format("fs.%s.impl.disable.cache",
                FileSystem.getDefaultUri(config).getScheme());
        config.set(disableCacheName, "true");

        // If kerberos is enabled, create the file system as the kerberos principal
        // -- use RESOURCE_LOCK to guarantee UserGroupInformation is accessed by only a single thread at at time
        FileSystem fs;
        UserGroupInformation ugi;
        synchronized (RESOURCES_LOCK) {
            if (SecurityUtil.isSecurityEnabled(config)) {
                ugi = SecurityUtil.loginKerberos(config,
                        connectorConfig.getKerberosUserPrincipal(), connectorConfig.getKeytab());
                fs = getFileSystemAsUser(config, ugi);
            } else {
                config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
                config.set("hadoop.security.authentication", "simple");
                ugi = SecurityUtil.loginSimple(config);
                fs = getFileSystemAsUser(config, ugi);
            }
        }
        return new HdfsResources(config, fs, ugi);
    }

    private static Configuration getConfig(final Configuration config, String res) throws IOException {
        boolean foundResources = false;
        if (null != res) {
            String[] resources = res.split(",");
            for (String resource : resources) {
                config.addResource(new Path(resource.trim()));
                foundResources = true;
            }
        }

        if (!foundResources) {
            // check that at least 1 non-default resource is available on the classpath
            String configStr = config.toString();
            for (String resource : configStr.substring(configStr.indexOf(":") + 1).split(",")) {
                if (!resource.contains("default") && config.getResource(resource.trim()) != null) {
                    foundResources = true;
                    break;
                }
            }
        }

        if (!foundResources) {
            throw new IOException("Could not find any of the " + res + " on the classpath");
        }
        return config;
    }

    /*
     * Reduce the timeout of a socket connection from the default in FileSystem.get()
     */
    protected void checkHdfsUriForTimeout(Configuration config) throws IOException {
        URI hdfsUri = FileSystem.getDefaultUri(config);
        String address = hdfsUri.getAuthority();
        int port = hdfsUri.getPort();
        if (address == null || address.isEmpty() || port < 0) {
            return;
        }
        InetSocketAddress namenode = NetUtils.createSocketAddr(address, port);
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(config);
        try (Socket socket = socketFactory.createSocket()) {
            NetUtils.connect(socket, namenode, 1000); // 1 second timeout
        }
    }

    /**
     * This exists in order to allow unit tests to override it so that they don't take several
     * minutes waiting for UDP packets to be received.
     *
     * @param config
     *            the configuration to use
     * @return the FileSystem that is created for the given Configuration
     * @throws IOException
     *             if unable to create the FileSystem
     */
    protected FileSystem getFileSystem(final Configuration config) throws IOException {
        return FileSystem.get(config);
    }

    protected FileSystem getFileSystemAsUser(final Configuration config, UserGroupInformation ugi) throws IOException {
        try {
            return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(config));
        } catch (InterruptedException e) {
            throw new IOException("Unable to create file system: " + e.getMessage());
        }
    }

    protected Configuration getConfiguration() {
        return hdfsResources.get().getConfiguration();
    }

    protected FileSystem getFileSystem() {
        return hdfsResources.get().getFileSystem();
    }

    protected UserGroupInformation getUserGroupInformation() {
        return hdfsResources.get().getUserGroupInformation();
    }

    protected String getEncoding() {
        return StringUtils.isNotBlank(connectorConfig.getEncoding())
                   ? connectorConfig.getEncoding() : Charset.defaultCharset().name();
    }

    protected CompressionCodec getCompressionCodec() {
       if (connectorConfig.getCompression() == null) {
           return null;
       }

       CompressionCodec codec = getCompressionCodecFactory()
               .getCodecByName(connectorConfig.getCompression().name());

       return (codec != null) ? codec : new DefaultCodec();
    }

    protected CompressionCodecFactory getCompressionCodecFactory() {
        if (compressionCodecFactory == null) {
            compressionCodecFactory = new CompressionCodecFactory(getConfiguration());
        }

        return compressionCodecFactory;
    }

    /**
     * Extending Hadoop Configuration to prevent it from caching classes that can't be found. Since users may be
     * adding additional JARs to the classpath we don't want them to have to restart the JVM to be able to load
     * something that was previously not found, but might now be available.
     * Reference the original getClassByNameOrNull from Configuration.
     */
    static class ExtendedConfiguration extends Configuration {

        private final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> cacheClasses = new WeakHashMap<>();

        @Override
        public Class<?> getClassByNameOrNull(String name) {
            final ClassLoader classLoader = getClassLoader();

            Map<String, WeakReference<Class<?>>> map;
            synchronized (cacheClasses) {
                map = cacheClasses.get(classLoader);
                if (map == null) {
                    map = Collections.synchronizedMap(new WeakHashMap<>());
                    cacheClasses.put(classLoader, map);
                }
            }

            Class<?> clazz = null;
            WeakReference<Class<?>> ref = map.get(name);
            if (ref != null) {
                clazz = ref.get();
            }

            if (clazz == null) {
                try {
                    clazz = Class.forName(name, true, classLoader);
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    return null;
                }
                // two putters can race here, but they'll put the same class
                map.put(name, new WeakReference<>(clazz));
                return clazz;
            } else {
                // cache hit
                return clazz;
            }
        }

    }
}
