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
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Auto refresher and builder of SSLContext.
 *
 * @param <T>
 *            type of SSLContext
 */
@Slf4j
public abstract class SslContextAutoRefreshBuilder<T> {
    protected final long refreshTime;
    protected long lastRefreshTime;

    public SslContextAutoRefreshBuilder(
            long certRefreshInSec) {
        this.refreshTime = TimeUnit.SECONDS.toMillis(certRefreshInSec);
        this.lastRefreshTime = -1;

        if (log.isDebugEnabled()) {
            log.debug("Certs will be refreshed every {} seconds", certRefreshInSec);
        }
    }

    /**
     * updates and returns cached SSLContext.
     *
     * @return
     * @throws GeneralSecurityException
     * @throws IOException
     */
    protected abstract T update() throws GeneralSecurityException, IOException;

    /**
     * Returns cached SSLContext.
     *
     * @return
     */
    protected abstract T getSslContext();

    /**
     * Returns whether the key files modified after a refresh time, and context need update.
     *
     * @return true if files modified
     */
    protected abstract boolean needUpdate();

    /**
     * It updates SSLContext at every configured refresh time and returns updated SSLContext.
     *
     * @return
     */
    public T get() {
        T ctx = getSslContext();
        if (ctx == null) {
            try {
                update();
                lastRefreshTime = System.currentTimeMillis();
                return getSslContext();
            } catch (GeneralSecurityException | IOException e) {
                log.error("Exception while trying to refresh ssl Context {}", e.getMessage(), e);
            }
        } else {
            long now = System.currentTimeMillis();
            if (refreshTime <= 0 || now > (lastRefreshTime + refreshTime)) {
                if (needUpdate()) {
                    try {
                        ctx = update();
                        lastRefreshTime = now;
                    } catch (GeneralSecurityException | IOException e) {
                        log.error("Exception while trying to refresh ssl Context {} ", e.getMessage(), e);
                    }
                }
            }
        }
        return ctx;
    }
}
