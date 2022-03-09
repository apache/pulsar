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
package org.apache.pulsar.tests;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.testng.ITestResult;
import org.testng.SkipException;
import org.testng.util.RetryAnalyzerCount;

public class RetryAnalyzer extends RetryAnalyzerCount {
    // Only try again once
    static final int MAX_RETRIES = Integer.parseInt(System.getProperty("testRetryCount", "1"));

    // Don't retry test classes that are changed in the current changeset in CI
    private static final Pattern TEST_FILE_PATTERN = Pattern.compile("^.*src/test/java/(.*)\\.java$");
    private static final Set<String> CHANGED_TEST_CLASSES = Optional.ofNullable(System.getenv("CHANGED_TESTS"))
            .map(changedTestsCsv ->
                    Collections.unmodifiableSet(Arrays.stream(StringUtils.split(changedTestsCsv))
                            .map(path -> {
                                Matcher matcher = TEST_FILE_PATTERN.matcher(path);
                                if (matcher.matches()) {
                                    return matcher.group(1)
                                            .replace('/', '.');
                                } else {
                                    return null;
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet())))
            .orElse(Collections.emptySet());

    public RetryAnalyzer() {
        setCount(MAX_RETRIES);
    }

    @Override
    public boolean retry(ITestResult result) {
        if (CHANGED_TEST_CLASSES.contains((result.getTestClass().getName()))) {
            return false;
        }
        return super.retry(result);
    }

    @Override
    public boolean retryMethod(ITestResult result) {
        return !(result.getThrowable() instanceof SkipException);
    }
}
