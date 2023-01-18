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
package org.apache.pulsar.common.util;

import static java.util.Objects.requireNonNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Static convenience URI checker.
 */
@ThreadSafe
public class URIPreconditions {

    /**
     * Check whether the given string is a legal URI and passes the user's check.
     *
     * @param uri       URI String
     * @param predicate User defined rule
     * @throws IllegalArgumentException Illegal URI or failed in the user's rules
     */
    public static void checkURI(@Nonnull String uri,
                                @Nonnull Predicate<URI> predicate) throws IllegalArgumentException {
        checkURI(uri, predicate, null);
    }

    /**
     * Check whether the given string is a legal URI and passes the user's check.
     *
     * @param uri       URI String
     * @param predicate User defined rule
     * @throws IllegalArgumentException Illegal URI or failed in the user's rules
     */
    public static void checkURIIfPresent(@Nullable String uri,
                                         @Nonnull Predicate<URI> predicate) throws IllegalArgumentException {
        checkURIIfPresent(uri, predicate, null);
    }

    /**
     * Check whether the given string is a legal URI and passes the user's check.
     *
     * @param uri          URI String
     * @param predicate    User defined rule
     * @param errorMessage Error message
     * @throws IllegalArgumentException Illegal URI or failed in the user's rules
     */
    public static void checkURIIfPresent(@Nullable String uri,
                                         @Nonnull Predicate<URI> predicate,
                                         @Nullable String errorMessage) throws IllegalArgumentException {
        if (uri == null) {
            return;
        }
        checkURI(uri, predicate, errorMessage);
    }

    /**
     * Check whether the given string is a legal URI and passes the user's check.
     *
     * @param uri          URI String
     * @param predicate    User defined rule
     * @param errorMessage Error message
     * @throws IllegalArgumentException Illegal URI or failed in the user's rules
     */
    public static void checkURI(@Nonnull String uri,
                                @Nonnull Predicate<URI> predicate,
                                @Nullable String errorMessage) throws IllegalArgumentException {
        requireNonNull(uri, "uri");
        requireNonNull(predicate, "predicate");
        try {
            URI u = new URI(uri);
            if (!predicate.test(u)) {
                throw new IllegalArgumentException(errorMessage == null ? "Illegal syntax: " + uri : errorMessage);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(errorMessage == null ? "Illegal syntax: " + uri : errorMessage);
        }
    }
}
