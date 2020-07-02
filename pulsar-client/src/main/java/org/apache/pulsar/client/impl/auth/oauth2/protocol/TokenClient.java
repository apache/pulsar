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
package org.apache.pulsar.client.impl.auth.oauth2.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * A client for an OAuth 2.0 token endpoint.
 */
public class TokenClient implements AutoCloseable, ClientCredentialsExchanger {

    private static final ObjectReader resultReader;
    private static final ObjectReader errorReader;

    static {
        resultReader = new ObjectMapper().readerFor(TokenResult.class);
        errorReader = new ObjectMapper().readerFor(TokenError.class);
    }

    private final URL tokenUrl;
    private final CloseableHttpClient httpclient;

    public TokenClient(URL tokenUrl) {
        this.tokenUrl = tokenUrl;
        this.httpclient = HttpClientBuilder.create().useSystemProperties().disableCookieManagement().build();
    }

    public void close() {
    }

    /**
     * Performs a token exchange using client credentials.
     * @param req the client credentials request details.
     * @return a token result
     * @throws TokenExchangeException
     */
    public TokenResult exchangeClientCredentials(ClientCredentialsExchangeRequest req)
            throws TokenExchangeException, IOException {
        List<NameValuePair> params = new ArrayList<>(4);
        params.add(new BasicNameValuePair("grant_type", "client_credentials"));
        params.add(new BasicNameValuePair("client_id", req.getClientId()));
        params.add(new BasicNameValuePair("client_secret", req.getClientSecret()));
        params.add(new BasicNameValuePair("audience", req.getAudience()));
        HttpPost post = new HttpPost(tokenUrl.toString());
        post.setHeader("Accept", ContentType.APPLICATION_JSON.getMimeType());
        post.setEntity(new UrlEncodedFormEntity(params, Consts.UTF_8));

        try (CloseableHttpResponse response = httpclient.execute(post)) {
            StatusLine status = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            try {
                switch(status.getStatusCode()) {
                    case HttpURLConnection.HTTP_OK:
                        return readResponse(entity, resultReader);
                    case HttpURLConnection.HTTP_BAD_REQUEST:
                    case HttpURLConnection.HTTP_UNAUTHORIZED:
                        throw new TokenExchangeException(readResponse(entity, errorReader));
                    default:
                        throw new HttpResponseException(status.getStatusCode(), status.getReasonPhrase());
                }
            } finally {
                EntityUtils.consume(entity);
            }
        }
    }

    private static <T> T readResponse(HttpEntity entity, ObjectReader objectReader) throws IOException {
        ContentType contentType = ContentType.getOrDefault(entity);
        if (!ContentType.APPLICATION_JSON.getMimeType().equals(contentType.getMimeType())) {
            throw new ClientProtocolException("Unsupported content type: " + contentType.getMimeType());
        }
        Charset charset = contentType.getCharset();
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        try (Reader reader = new InputStreamReader(entity.getContent(), charset)) {
            @SuppressWarnings("unchecked") T obj = (T) objectReader.readValue(reader);
            return obj;
        }
    }
}
