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
package org.apache.pulsar.common.intercept;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.CustomLog;
import org.apache.pulsar.common.util.ClassLoaderUtils;

/**
 * A tool class for loading BrokerEntryMetadataInterceptor classes.
 */
@CustomLog
public class BrokerEntryMetadataUtils<T> {

    public static Set<BrokerEntryMetadataInterceptor> loadBrokerEntryMetadataInterceptors(
            Set<String> interceptorNames, ClassLoader classLoader) {
        Set<BrokerEntryMetadataInterceptor> interceptors = new HashSet<>();
        if (interceptorNames != null && interceptorNames.size() > 0) {
            for (String interceptorName : interceptorNames) {
                try {
                    @SuppressWarnings("unchecked") // class is loaded by name and expected to implement the interface
                    Class<BrokerEntryMetadataInterceptor> clz = (Class<BrokerEntryMetadataInterceptor>) ClassLoaderUtils
                            .loadClass(interceptorName, classLoader);
                    try {
                        interceptors.add(clz.getDeclaredConstructor().newInstance());
                    } catch (InstantiationException | IllegalAccessException
                            | InvocationTargetException | NoSuchMethodException e) {
                        log.error()
                                .attr("interceptorName", interceptorName)
                                .exception(e)
                                .log("Create new BrokerEntryMetadataInterceptor instance failed.");
                        throw new RuntimeException(e);
                    }
                } catch (ClassNotFoundException e) {
                    log.error()
                            .attr("interceptorName", interceptorName)
                            .exception(e)
                            .log("Load BrokerEntryMetadataInterceptor class failed.");
                    throw new RuntimeException(e);
                }
            }
        }
        return interceptors;
    }
    public static <T> Set<T> loadInterceptors(
            Set<String> interceptorNames, ClassLoader classLoader) {
        Set<T> interceptors = new LinkedHashSet<>();
        if (interceptorNames != null && interceptorNames.size() > 0) {
            for (String interceptorName : interceptorNames) {
                try {
                    @SuppressWarnings("unchecked") // class is loaded by name and expected to match type T
                    Class<T> clz = (Class<T>) ClassLoaderUtils
                        .loadClass(interceptorName, classLoader);
                    try {
                        interceptors.add(clz.getDeclaredConstructor().newInstance());
                    } catch (InstantiationException | IllegalAccessException
                        | InvocationTargetException | NoSuchMethodException e) {
                        log.error()
                            .attr("interceptorName", interceptorName)
                            .exception(e)
                            .log("Create new instance failed.");
                        throw new RuntimeException(e);
                    }
                } catch (ClassNotFoundException e) {
                    log.error()
                        .attr("interceptorName", interceptorName)
                        .exception(e)
                        .log("Load class failed.");
                    throw new RuntimeException(e);
                }
            }
        }
        return interceptors;
    }
}
