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
package org.apache.pulsar.io.flume.node;

import com.google.common.base.Preconditions;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A class that extends the Java built-in Properties overriding.
 * {@link java.util.Properties#getProperty(String)} to allow ${ENV_VAR_NAME}-style environment
 * variable inclusions
 */
public class EnvVarResolverProperties extends Properties {
    /**
     * @param input The input string with ${ENV_VAR_NAME}-style environment variable names
     * @return The output string with ${ENV_VAR_NAME} replaced with their environment variable values
     */
    protected static String resolveEnvVars(String input) {
        Preconditions.checkNotNull(input);
        // match ${ENV_VAR_NAME}
        Pattern p = Pattern.compile("\\$\\{(\\w+)\\}");
        Matcher m = p.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String envVarName = m.group(1);
            String envVarValue = System.getenv(envVarName);
            m.appendReplacement(sb, null == envVarValue ? "" : envVarValue);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * @param key the property key
     * @return the value of the property key with ${ENV_VAR_NAME}-style environment variables replaced
     */
    @Override
    public String getProperty(String key) {
        return resolveEnvVars(super.getProperty(key));
    }
}
