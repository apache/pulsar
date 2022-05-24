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

import java.util.EnumSet;
import java.util.Map;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;

public class Filters {
    private static final String MATCH_ALL = "/*";

    /**
     * Adds a filter instance to the servlet context handler.
     * The filter will be used for all requests.
     *
     * @param context servlet context handler instance
     * @param filter filter instance
     */
    public static void addFilter(ServletContextHandler context, Filter filter) {
        addFilterHolder(context, new FilterHolder(filter));
    }

    private static void addFilterHolder(ServletContextHandler context, FilterHolder filter) {
        context.addFilter(filter,
                MATCH_ALL, EnumSet.allOf(DispatcherType.class));
    }

    /**
     * Adds a filter to the servlet context handler which gets instantiated and configured when the server starts.
     *
     * @param context servlet context handler instance
     * @param filter filter class
     * @param initParams initialization parameters used for configuring the filter instance
     */
    public static void addFilterClass(ServletContextHandler context, Class<? extends Filter> filter,
                                      Map<String, String> initParams) {
        FilterHolder holder = new FilterHolder(filter);
        holder.setInitParameters(initParams);
        addFilterHolder(context, holder);
    }
}
