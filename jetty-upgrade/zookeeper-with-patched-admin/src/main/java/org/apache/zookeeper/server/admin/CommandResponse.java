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
package org.apache.zookeeper.server.admin;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

/**
 * A response from running a {@link Command}.
 */
public class CommandResponse {

    /**
     * The key in the map returned by {@link #toMap()} for the command name.
     */
    public static final String KEY_COMMAND = "command";
    /**
     * The key in the map returned by {@link #toMap()} for the error string.
     */
    public static final String KEY_ERROR = "error";

    private final String command;
    private final String error;
    private final Map<String, Object> data;
    private final Map<String, String> headers;
    private int statusCode;
    private InputStream inputStream;

    /**
     * Creates a new response with no error string.
     *
     * @param command command name
     */
    public CommandResponse(String command) {
        this(command, null, HttpServletResponse.SC_OK);
    }

    /**
     * Creates a new response.
     *
     * @param command    command name
     * @param error      error string (may be null)
     * @param statusCode http status code
     */
    public CommandResponse(String command, String error, int statusCode) {
        this(command, error, statusCode, null);
    }


    /**
     * Creates a new response.
     *
     * @param command     command name
     * @param error       error string (may be null)
     * @param statusCode  http status code
     * @param inputStream inputStream to send out data (may be null)
     */
    public CommandResponse(final String command, final String error, final int statusCode,
                           final InputStream inputStream) {
        this.command = command;
        this.error = error;
        data = new LinkedHashMap<>();
        headers = new HashMap<>();
        this.statusCode = statusCode;
        this.inputStream = inputStream;
    }

    /**
     * Gets the command name.
     *
     * @return command name
     */
    public String getCommand() {
        return command;
    }

    /**
     * Gets the error string (may be null).
     *
     * @return error string
     */
    public String getError() {
        return error;
    }

    /**
     * Gets the http status code.
     *
     * @return http status code
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * Sets the http status code.
     */
    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    /**
     * Gets the InputStream (may be null).
     *
     * @return InputStream
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * Sets the InputStream.
     */
    public void setInputStream(final InputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Adds a key/value pair to this response.
     *
     * @param key   key
     * @param value value
     * @return prior value for key, or null if none
     */
    public Object put(String key, Object value) {
        return data.put(key, value);
    }

    /**
     * Adds all key/value pairs in the given map to this response.
     *
     * @param m map of key/value pairs
     */
    public void putAll(Map<? extends String, ?> m) {
        data.putAll(m);
    }

    /**
     * Adds a header to this response.
     *
     * @param name  name of the header
     * @param value value of the header
     */
    public void addHeader(final String name, final String value) {
        headers.put(name, value);
    }

    /**
     * Returns all headers.
     *
     * @return map representation of all headers
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Converts this response to a map. The returned map is mutable, and
     * changes to it do not reflect back into this response.
     *
     * @return map representation of response
     */
    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>(data);
        m.put(KEY_COMMAND, command);
        m.put(KEY_ERROR, error);
        m.putAll(data);
        return m;
    }

}
