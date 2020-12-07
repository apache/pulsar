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
package org.apache.pulsar.broker.service.dispatcher;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Util class to load customized message dispatcher.
 */
@Slf4j
public class DispatcherUtils {

    // Classloader
    private static NarClassLoader ncl = null;

    // Cache class definition.
    private static Map<String, Class> dispatchers;

    /**
     * Initialization.
     * @param narPath Path for the nar containing customized dispatcher we need to load.
     * @throws IOException
     */
    public static void init(String narPath)  throws IOException {
        if (narPath != null) {
            ncl = NarClassLoader.getFromArchive(
                    new File(narPath),
                    Collections.emptySet());
            dispatchers = new HashMap<>();
        }
    }

    /**
     * Load the customized {@link Dispatcher} according to the class name.
     */
    public static Dispatcher load(String dispatcherClassName) throws IOException {
        checkArgument(ncl != null);
        if (StringUtils.isBlank(dispatcherClassName)) {
            throw new IOException("Dispatcher class name can not be empty");
        }

        if (!dispatchers.containsKey(dispatcherClassName)) {
            synchronized (DispatcherUtils.class) {
                if (!dispatchers.containsKey(dispatcherClassName)) {
                    try {
                        Class dispatcherClazz = ncl.loadClass(dispatcherClassName);
                        dispatchers.put(dispatcherClassName, dispatcherClazz);
                    } catch (Throwable t) {
                        rethrowIOException(t);
                    }
                }
            }
        }
        Object dispatcher;
        try {
            dispatcher = dispatchers.get(dispatcherClassName).newInstance();
            if (!(dispatcher instanceof Dispatcher)) {
                throw new RuntimeException("Class " + dispatcher.getClass()
                        + " does not implement dispatcher interface");
            }
            return (Dispatcher) dispatcher;
        } catch (Throwable t) {
            rethrowIOException(t);
            return null;
        }
    }

    public static void close() {
        if (ncl != null) {
            try {
                ncl.close();
            } catch (IOException e) {
                log.warn("Failed to close dispatcher class loader", e);
            }
        }
    }

    private static void rethrowIOException(Throwable cause)
            throws IOException {
        if (cause instanceof IOException) {
            throw (IOException) cause;
        } else if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
            throw (Error) cause;
        } else {
            throw new IOException(cause.getMessage(), cause);
        }
    }

}
