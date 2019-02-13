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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import io.netty.handler.ssl.SslContext;

public abstract class SslContextRefresher implements Runnable {
    private final Set<String> filesToWatch;
    private final Set<WatchKey> watchKeys;
    private final long delay;
    private final TimeUnit timeunit;
    private final ScheduledExecutorService eventLoopGroup;

    public SslContextRefresher(ScheduledExecutorService eventLoopGroup, long delay, TimeUnit timeunit) {
        this.delay = delay;
        this.timeunit = timeunit;
        this.watchKeys = Sets.newHashSet();
        this.filesToWatch = Sets.newHashSet();
        this.eventLoopGroup = eventLoopGroup;

        if (delay > 0) {
            run();
        }
    }

    @Override
    public void run() {
        for (WatchKey key : watchKeys) {
            List<WatchEvent<?>> events = key.pollEvents();
            for (WatchEvent<?> e : events) {
                String file = ((Path) key.watchable()).toFile().getAbsolutePath();
                Kind<?> kind = e.kind();
                if (kind.equals(StandardWatchEventKinds.OVERFLOW)) {
                    LOG.warn("Overflow occured for file {} - rebuilding sslContext", file);
                    buildSSLContext();
                    break;
                } else if (filesToWatch.contains(file)) {
                    LOG.info("Changed found for file {} and EventType {} - rebuilding sslContext", file, kind);
                    buildSSLContext();
                    break;
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring changes for file {}, EventType {}.", file, kind);
                }
            }
        }
        this.eventLoopGroup.schedule(this, this.delay, this.timeunit);
    }

    public void registerFile(String... files) throws IOException {
        for (int i = 0; i < files.length; i++) {
            String file = files[i];
            LOG.info("Registering File {}", file);
            if (! Strings.isNullOrEmpty(file)) {
                filesToWatch.add(file);
                Path p = Paths.get(file).getParent();
                WatchService watchService = p.getFileSystem().newWatchService();
                WatchKey key = p.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.OVERFLOW);
                watchKeys.add(key);
            }
        }
    }

    public abstract void buildSSLContext();

    public abstract SslContext get();

    private static final Logger LOG = LoggerFactory.getLogger(SslContextRefresher.class);
}
