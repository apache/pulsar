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
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.SkipException;
import org.testng.internal.thread.ThreadTimeoutException;

public class PulsarTestListener implements ITestListener {

    @Override
    public void onTestStart(ITestResult result) {
        System.out.format("------- Starting test %s.%s(%s)-------\n", result.getTestClass(),
                result.getMethod().getMethodName(), Arrays.toString(result.getParameters()));
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        System.out.format("------- SUCCESS -- %s.%s(%s)-------\n", result.getTestClass(),
                result.getMethod().getMethodName(), Arrays.toString(result.getParameters()));
    }

    @Override
    public void onTestFailure(ITestResult result) {
        if (!(result.getThrowable() instanceof SkipException)) {
            System.out.format("!!!!!!!!! FAILURE-- %s.%s(%s)-------\n", result.getTestClass(),
                    result.getMethod().getMethodName(), Arrays.toString(result.getParameters()));
        }
        if (result.getThrowable() instanceof ThreadTimeoutException) {
            System.out.println("====== THREAD DUMPS ======");
            System.out.println(ThreadDumpUtil.buildThreadDiagnosticString());
        }
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        System.out.format("~~~~~~~~~ SKIPPED -- %s.%s(%s)-------\n", result.getTestClass(),
                result.getMethod().getMethodName(), Arrays.toString(result.getParameters()));
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {

    }

    @Override
    public void onStart(ITestContext context) {

    }

    @Override
    public void onFinish(ITestContext context) {
    }
}
