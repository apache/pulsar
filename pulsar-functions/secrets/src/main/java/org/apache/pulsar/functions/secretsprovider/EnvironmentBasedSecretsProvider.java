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
package org.apache.pulsar.functions.secretsprovider;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a very simple Secrets Provider that looks up environment variable
 * thats named the same as secretName and fetches it.
 */
public class EnvironmentBasedSecretsProvider implements SecretsProvider {

    /**
     * Pattern to match ${secretName} in the value.
     */
    private static final Pattern interpolationPattern = Pattern.compile("\\$\\{(.+?)}");

    /**
     * Fetches a secret.
     *
     * @return The actual secret
     */
    @Override
    public String provideSecret(String secretName, Object pathToSecret) {
        return System.getenv(secretName);
    }

    @Override
    public String interpolateSecretForValue(String value) {
        Matcher m = interpolationPattern.matcher(value);
        if (m.matches()) {
            String secretName = m.group(1);
            // If the secret doesn't exist, we return null and don't override the current value.
            return provideSecret(secretName, null);
        }
        return null;
    }
}