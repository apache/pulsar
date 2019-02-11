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
package org.apache.pulsar.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

public class SslContextRefresher implements Runnable {
    private final boolean tlsAllowInsecureConnection;
    private final String tlsTrustCertsFilePath;
    private final String tlsCertificateFilePath;
    private final String tlsKeyFilePath;
    private final Set<String> tlsCiphers;
    private final Set<String> tlsProtocols;
    private final boolean tlsRequireTrustedClientCertOnConnect;
    private final EventLoopGroup eventLoopGroup;
    private final Set<String> files;
    private final Set<WatchKey> watchKeys;
    private final long delay;
    private final TimeUnit timeunit;
    private volatile SslContext sslContext;

    public SslContextRefresher(boolean allowInsecure, String trustCertsFilePath, String certificateFilePath,
            String keyFilePath, Set<String> ciphers, Set<String> protocols, boolean requireTrustedClientCertOnConnect,
            EventLoopGroup eventLoopGroup, long delay, TimeUnit timeunit)
            throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = trustCertsFilePath;
        this.tlsCertificateFilePath = certificateFilePath;
        this.tlsKeyFilePath = keyFilePath;
        this.tlsCiphers = ciphers;
        this.tlsProtocols = protocols;
        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
        this.watchKeys = Sets.newHashSet();
        this.files = Sets.newHashSet();
        this.delay = delay;
        this.timeunit = timeunit;
        this.eventLoopGroup = eventLoopGroup;

        files.add(trustCertsFilePath);
        files.add(certificateFilePath);
        files.add(keyFilePath);
        
        // Remove all Null Or Empty Files
        files.removeIf(file -> Strings.isNullOrEmpty(file));
        
        this.sslContext = SecurityUtility.createNettySslContextForServer(tlsAllowInsecureConnection,
                tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath, tlsCiphers, tlsProtocols,
                tlsRequireTrustedClientCertOnConnect);

        for (String file : files) {
            LOG.info("File {}", file);
            Path p = Paths.get(file).getParent();
            WatchService watchService = p.getFileSystem().newWatchService();
            WatchKey key = p.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.OVERFLOW);
            watchKeys.add(key);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Certs will be refreshed every {} minutes", delay);
        }
        
        if (delay > 0) {
            run();
        }
    }

    public void run() {
        for (WatchKey key : watchKeys) {
            List<WatchEvent<?>> events = key.pollEvents();
            for (WatchEvent<?> e : events) {
                String file = ((Path) key.watchable()).toFile().getAbsolutePath();
                if (e.kind().equals(StandardWatchEventKinds.OVERFLOW)) {
                    LOG.warn("Overflow occured for file {} - rebuilding sslContext", file);
                    buildSSLContext();
                    break;
                } else if (files.contains(file)) {
                    LOG.info("Changed found for file {} - rebuilding sslContext", file);
                    buildSSLContext();
                    break;
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring changes for file {}.", file);
                }
            }
        }
        this.eventLoopGroup.schedule(this, this.delay, this.timeunit);
    }

    public void buildSSLContext() {
        SslContext sslContext;
        try {
            sslContext = SecurityUtility.createNettySslContextForServer(tlsAllowInsecureConnection,
                    tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath, tlsCiphers, tlsProtocols,
                    tlsRequireTrustedClientCertOnConnect);
        } catch (GeneralSecurityException | IOException e) {
            LOG.error("Error occured while trying to create sslContext - using previous one.");
            return;
        }
        this.sslContext = sslContext;
    }

    public SslContext get() {
        return this.sslContext;
    }

    private static final Logger LOG = LoggerFactory.getLogger(SslContextRefresher.class);
}
