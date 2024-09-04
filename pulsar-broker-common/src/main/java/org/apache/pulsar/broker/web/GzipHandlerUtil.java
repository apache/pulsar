/*
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

import java.util.List;
import org.eclipse.jetty.http.pathmap.PathSpecSet;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.IncludeExclude;

public class GzipHandlerUtil {
    public static Handler wrapWithGzipHandler(Handler innerHandler, List<String> gzipCompressionExcludedPaths) {
        Handler wrappedHandler;
        if (isGzipCompressionCompletelyDisabled(gzipCompressionExcludedPaths)) {
            // no need to add GZIP handler if it's disabled by setting the excluded path to "^.*" or "^.*$"
            wrappedHandler = innerHandler;
        } else {
            // add GZIP handler which is active when the request contains "Accept-Encoding: gzip" header
            GzipHandler gzipHandler = new GzipHandler();
            gzipHandler.setHandler(innerHandler);
            if (gzipCompressionExcludedPaths != null && gzipCompressionExcludedPaths.size() > 0) {
                gzipHandler.setExcludedPaths(gzipCompressionExcludedPaths.toArray(new String[0]));
            }
            wrappedHandler = gzipHandler;
        }
        return wrappedHandler;
    }

    public static boolean isGzipCompressionCompletelyDisabled(List<String> gzipCompressionExcludedPaths) {
        return gzipCompressionExcludedPaths != null && gzipCompressionExcludedPaths.size() == 1
                && (gzipCompressionExcludedPaths.get(0).equals("^.*")
                || gzipCompressionExcludedPaths.get(0).equals("^.*$"));
    }

    /**
     * Check if GZIP compression is enabled for the given endpoint.
     * @param gzipCompressionExcludedPaths list of paths that should not be compressed
     * @param endpoint the endpoint to check
     * @return true if GZIP compression is enabled for the endpoint, false otherwise
     */
    public static boolean isGzipCompressionEnabledForEndpoint(List<String> gzipCompressionExcludedPaths,
                                                              String endpoint) {
        if (gzipCompressionExcludedPaths == null || gzipCompressionExcludedPaths.isEmpty()) {
            return true;
        }
        if (isGzipCompressionCompletelyDisabled(gzipCompressionExcludedPaths)) {
            return false;
        }
        IncludeExclude<String> paths = new IncludeExclude<>(PathSpecSet.class);
        paths.exclude(gzipCompressionExcludedPaths.toArray(new String[0]));
        return paths.test(endpoint);
    }
}
