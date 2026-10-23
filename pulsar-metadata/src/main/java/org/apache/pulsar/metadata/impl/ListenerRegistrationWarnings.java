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
package org.apache.pulsar.metadata.impl;

import io.github.merlimat.slog.Logger;
import java.lang.reflect.Proxy;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.metadata.api.MetadataStore;

/**
 * Logs the warnings of the deprecated listener registration methods of {@link MetadataStore}, once per class that
 * has to change, so that a plugin that registers listeners often doesn't flood the log. Internal to the metadata store
 * API.
 */
public final class ListenerRegistrationWarnings {

    private static final Logger LOG = Logger.get(ListenerRegistrationWarnings.class);
    private static final StackWalker STACK_WALKER =
            StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    // The method and the class that were warned about
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

    private ListenerRegistrationWarnings() {
    }

    /**
     * Warn that a deprecated registration method was called, naming the class that called it.
     */
    public static void deprecatedMethodCalled(String method, String replacement) {
        Class<?> caller = findCaller();
        if (WARNED.add(method + " " + caller.getName())) {
            LOG.warn().attr("method", method).attr("caller", caller.getName()).attr("replacement", replacement)
                    .log("A deprecated MetadataStore listener registration method was called, the listener can't be "
                            + "unregistered. Use the replacement and run the callback it returns when the listener is "
                            + "no longer needed");
        }
    }

    /**
     * Warn that a store implementation doesn't implement a cancellable registration method.
     */
    public static void cancellableMethodNotImplemented(Class<?> storeClass, String method) {
        if (WARNED.add(method + " " + storeClass.getName())) {
            LOG.warn().attr("method", method).attr("store", storeClass.getName())
                    .log("The MetadataStore implementation doesn't implement the cancellable listener registration "
                            + "method, its listeners can't be unregistered");
        }
    }

    // The first class on the stack that isn't part of the metadata store or of the JDK
    private static Class<?> findCaller() {
        return STACK_WALKER.walk(frames -> frames
                .<Class<?>>map(StackWalker.StackFrame::getDeclaringClass)
                .filter(c -> c != ListenerRegistrationWarnings.class
                        && !MetadataStore.class.isAssignableFrom(c)
                        && !Proxy.isProxyClass(c)
                        && !c.getName().startsWith("java.")
                        && !c.getName().startsWith("jdk.")
                        && !c.getName().startsWith("sun."))
                .findFirst()
                .orElse(ListenerRegistrationWarnings.class));
    }
}
