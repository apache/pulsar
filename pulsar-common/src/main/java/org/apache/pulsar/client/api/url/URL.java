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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.net.URLStreamHandlerFactory;

/**
 * Wrapper around {@code java.net.URL} to improve usability.
 */
public class URL {
    private static final URLStreamHandlerFactory urlStreamHandlerFactory = new PulsarURLStreamHandlerFactory();
    private final java.net.URL url;

    public URL(String spec)
            throws MalformedURLException, URISyntaxException, InstantiationException, IllegalAccessException {
        String scheme = new URI(spec).getScheme();
        if (scheme == null) {
            this.url = new java.net.URL(null, "file:" + spec);
        } else {
            this.url = new java.net.URL(null, spec, urlStreamHandlerFactory.createURLStreamHandler(scheme));
        }
    }

    public URLConnection openConnection() throws IOException {
        return this.url.openConnection();
    }

    public Object getContent() throws IOException {
        return this.url.getContent();
    }

    public Object getContent(Class<?>[] classes) throws IOException {
        return this.url.getContent(classes);
    }

}
