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
package org.apache.pulsar.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PulsarBrokerVersionStringUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarBrokerVersionStringUtils.class);

    private static final String RESOURCE_NAME = "pulsar-broker-version.properties";
    private static final Pattern majorMinorPatchPattern = Pattern.compile("([1-9]+[0-9]*)\\.([1-9]+[0-9]*)\\.([1-9]+[0-9]*)(.*)");

    // If the version string does not contain a patch version, add one so the
    // version becomes valid according to the SemVer library (see https://github.com/zafarkhaja/jsemver).
    // This method (and it's only call above in the ctor) may be removed when SemVer accepts null patch versions
    public static String fixVersionString(String version) {
        if ( null == version ) {
            return null;
        }

        Matcher majorMinorPatchMatcher = majorMinorPatchPattern.matcher(version);

        if ( majorMinorPatchMatcher.matches() ) {
            // this is a valid version, containing a major, a minor, and a patch version (and optionally
            // a release candidate version and/or build metadata)
            return version;
        } else {
            // the patch version is missing, so add one ("0")
            Pattern pattern2 = Pattern.compile("([1-9]+[0-9]*)\\.([1-9]+[0-9]*)(.*)");
            Matcher matcher2 = pattern2.matcher(version);

            if (matcher2.matches()) {
                int startMajorVersion = matcher2.start(1);
                int stopMinorVersion = matcher2.end(2);
                int startReleaseCandidate = matcher2.start(3);

                String prefix = new String(version.getBytes(), startMajorVersion, (stopMinorVersion-startMajorVersion));
                String patchVersion = ".0";
                String suffix = new String(version.getBytes(), startReleaseCandidate, version.length() - startReleaseCandidate);

                return (prefix + patchVersion + suffix);
            } else {
                // This is an invalid version, let the JSemVer library fail when it parses it
                return version;
            }
        }
    }

    /**
     * Looks for a resource in the jar which is expected to be a java.util.Properties, then
     * extract a specific property value.
     *
     * @return the property value, or null if the resource does not exist or the resource
     *         is not a valid java.util.Properties or the resource does not contain the
     *         named property
     */
    private static String getPropertyFromResource(String resource, String propertyName) {
        try {
            InputStream stream = PulsarBrokerVersionStringUtils.class.getClassLoader().getResourceAsStream(resource);
            if (stream == null) {
                return null;
            }
            Properties properties = new Properties();
            try {
                properties.load(stream);
                String propertyValue = (String) properties.get(propertyName);
                return propertyValue;
            } catch (IOException e) {
                return null;
            } finally {
                stream.close();
            }
        } catch (Throwable t) {
            return null;
        }
    }

    public static String getNormalizedVersionString() {
        return fixVersionString(getPropertyFromResource(RESOURCE_NAME, "version"));
    }
}
