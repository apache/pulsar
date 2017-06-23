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
package org.apache.pulsar.broker.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.zookeeper.Deserializers;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a servlet {@code Filter} which rejects requests from clients older than the configured minimum
 * version.
 *
 */
public class ApiVersionFilter implements Filter {
    // Needed constants.
    private static final String CLIENT_VERSION_PARAM = "pulsar-client-version";
    private static final String MIN_API_VERSION_PATH = "/minApiVersion";
    private static final Logger LOG = LoggerFactory.getLogger(ApiVersionFilter.class);

    /**
     * The PulsarService instance holding the Local ZooKeeper cache. We use this rather than the underlying Zk cache
     * directly as the cache is not initialized at the time the ApiVersionFilter is constructed.
     */
    private final PulsarService pulsar;

    /** If true, clients which do not report a version will be allowed. */
    private final boolean allowUnversionedClients;

    public ApiVersionFilter(PulsarService pulsar, boolean allowUnversionedClients) {
        this.pulsar = pulsar;
        this.allowUnversionedClients = allowUnversionedClients;
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
            throws IOException, ServletException {
        try {
            String minApiVersion = pulsar.getLocalZkCache().getData(MIN_API_VERSION_PATH,
                    Deserializers.STRING_DESERIALIZER).orElseThrow(() -> new KeeperException.NoNodeException());
            String requestApiVersion = getRequestApiVersion(req);
            if (shouldAllowRequest(req.getRemoteAddr(), minApiVersion, requestApiVersion)) {
                // Allow the request to continue by invoking the next filter in
                // the chain.
                chain.doFilter(req, resp);
            } else {
                // The client's API version is less than the min supported,
                // reject the request.
                HttpServletResponse httpResponse = (HttpServletResponse) resp;
                HttpServletResponseWrapper respWrapper = new HttpServletResponseWrapper(httpResponse);
                respWrapper.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unsuported Client version");
            }
        } catch (Exception ex) {
            LOG.warn("[{}] Unable to safely determine client version eligibility. Allowing request",
                    req.getRemoteAddr());
            chain.doFilter(req, resp);
        }
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
        // No init necessary.
    }

    @Override
    public void destroy() {
        // No state to clean up.
    }

    /**
     * Checks to see if {@code requestApiVersion} is greater than {@code minApiVersion}. Does that by converting both
     * {@code minApiVersion} and {@code requestApiVersion} to a floating point number. Assumes that version are properly
     * formatted floating point numbers.
     *
     * Note that this scheme implies that version numbers cannot be of the format x.y.z or any other format which is not
     * a valid floating point number.
     *
     * @param minApiVersion
     * @param requestApiVersion
     * @return true if requestApiVersion is greater than or equal to minApiVersion
     */
    private boolean shouldAllowRequest(String clientAddress, String minApiVersion, String requestApiVersion) {
        if (requestApiVersion == null) {
            // The client has not sent a version, allow the request if
            // configured to do so.
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Checking client version: req: {} -- min: {} -- Allow unversioned: {}", clientAddress,
                        requestApiVersion, minApiVersion, allowUnversionedClients);
            }
            return allowUnversionedClients;
        }
        try {
            float minVersion = Float.parseFloat(minApiVersion);
            float requestVersion = Float.parseFloat(requestApiVersion);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Checking client version: req: {} -- min: {}", clientAddress, requestApiVersion,
                        minApiVersion);
            }
            return minVersion <= requestVersion;
        } catch (NumberFormatException ex) {
            LOG.warn("[{}] Unable to convert version info to floats. " + "minVersion = {}, requestVersion = {}",
                    clientAddress, minApiVersion, requestApiVersion);
            throw new IllegalArgumentException("Invalid Number in min or request API version");
        }
    }

    private String getRequestApiVersion(ServletRequest req) {
        // Implementation assumes that the client version is in an HTTP header
        // named client_version.
        // TODO (agh) Ensure that this is the case.
        HttpServletRequest httpReq = (HttpServletRequest) req;
        return httpReq.getHeader(CLIENT_VERSION_PARAM);
    }
}
