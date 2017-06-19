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
package org.testng.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class TestListener extends TestListenerAdapter {

    private static final Logger log = LoggerFactory.getLogger(TestListener.class);

    @Override
    public void beforeConfiguration(ITestResult tr) {
        log.info("%%%%%%%%%%%% Before configuration - {} / {} -- attrs: {}", tr.getTestClass().getName(),
                tr.getMethod().getMethodName(), tr.getAttributeNames());
    }

    @Override
    public void onTestSuccess(ITestResult tr) {
        log.info("++++++++++++ Test succeeded - {} / {} -- attrs: {}", tr.getTestClass().getName(),
                tr.getMethod().getMethodName(), tr.getAttributeNames());
    }

    @Override
    public void onTestFailure(ITestResult tr) {
        log.error("------------ Test Failed - {} / {} -- attrs: {}", tr.getTestClass().getName(),
                tr.getMethod().getMethodName(), tr.getAttributeNames(), tr.getThrowable());
    }

    @Override
    public void onTestSkipped(ITestResult tr) {
        log.warn("<<<<<<<<<<<< Test skipped - {} / {} -- attrs: {}", tr.getTestClass().getName(),
                tr.getMethod().getMethodName(), tr.getAttributeNames());
    }

    @Override
    public void onTestStart(ITestResult tr) {
        log.info("@@@@@@@@@@@@@ STARTING TEST - {} / {} -- attrs: {}", tr.getTestClass().getName(),
                tr.getMethod().getMethodName(), tr.getAttributeNames());
    }

}
