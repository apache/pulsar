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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataURLStreamHandler extends URLStreamHandler {

    class DataURLConnection extends URLConnection {
        private boolean parsed = false;
        private String contentType;
        private String data;
        private URI uri;

        protected DataURLConnection(URL url) {
            super(url);
            try {
                this.uri = this.url.toURI();
            } catch (URISyntaxException e) {
                this.uri = null;
            }
        }

        @Override
        public void connect() throws IOException {
            if (this.parsed) {
                return;
            }

            if (this.uri == null) {
                throw new IOException();
            }
            Pattern pattern = Pattern.compile(
                    "(?<mimeType>.+?)(;(?<charset>charset=.+?))?(;(?<base64>base64?))?,(?<data>.+)", Pattern.DOTALL);
            Matcher matcher = pattern.matcher(this.uri.getSchemeSpecificPart());
            if (matcher.matches()) {
                this.contentType = matcher.group("mimeType");
                String charset = matcher.group("charset");
                if (charset == null) {
                    charset = "US-ASCII";
                }
                if (matcher.group("base64") == null) {
                    // Support Urlencode but not decode here because already decoded by URI class.
                    this.data = new String(matcher.group("data").getBytes(), charset);
                } else {
                    this.data = new String(Base64.getDecoder().decode(matcher.group("data")), charset);
                }
            } else {
                throw new MalformedURLException();
            }
            parsed = true;
        }

        @Override
        public long getContentLengthLong() {
            long length;
            try {
                this.connect();
                length = this.data.length();
            } catch (IOException e) {
                length = -1;
            }
            return length;
        }

        @Override
        public String getContentType() {
            String contentType;
            try {
                this.connect();
                contentType = this.contentType;
            } catch (IOException e) {
                contentType = null;
            }
            return contentType;
        }

        @Override
        public String getContentEncoding() {
            return "identity";
        }

        public InputStream getInputStream() throws IOException {
            this.connect();
            return new ByteArrayInputStream(this.data.getBytes());
        }
    }

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
        return new DataURLConnection(u);
    }

}
