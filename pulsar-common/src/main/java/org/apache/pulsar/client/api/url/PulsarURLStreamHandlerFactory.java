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
package org.apache.pulsar.client.api.url;

import java.lang.reflect.InvocationTargetException;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * This class defines a factory for {@code URL} stream
 * protocol handlers.
 */
public class PulsarURLStreamHandlerFactory implements URLStreamHandlerFactory {
    private static final Map<String, Class<? extends URLStreamHandler>> handlers;
    static {
        handlers = new HashMap<>();
        handlers.put("data", DataURLStreamHandler.class);
    }

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        URLStreamHandler urlStreamHandler;
        try {
            Class<? extends URLStreamHandler> handler = handlers.get(protocol);
            if (handler != null) {
                urlStreamHandler = handler.getDeclaredConstructor().newInstance();
            } else {
                urlStreamHandler = null;
            }
        } catch (InstantiationException | IllegalAccessException
                | InvocationTargetException | NoSuchMethodException e) {
            urlStreamHandler = null;
        }
        return urlStreamHandler;
    }

}
