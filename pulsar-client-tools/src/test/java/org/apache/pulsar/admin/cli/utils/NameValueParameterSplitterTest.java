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
package org.apache.pulsar.admin.cli.utils;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NameValueParameterSplitterTest {
    @Test(description = "Basic Test")
    public void test1() {
        NameValueParameterSplitter splitter = new NameValueParameterSplitter();
        Map<String, String> result = splitter.convert("Name=Sunnyvale");
        Assert.assertEquals(result.get("Name"), "Sunnyvale");
    }

    @Test(description = "Check trimming of values")
    public void test2() {
        NameValueParameterSplitter splitter = new NameValueParameterSplitter();
        Map<String, String> result = splitter.convert(" Name = Sunnyvale CA");
        Assert.assertEquals(result.get("Name"), "Sunnyvale CA");
    }

    @Test(description = "Check error on invalid input")
    public void test3() {
        try {
            NameValueParameterSplitter splitter = new NameValueParameterSplitter();
            splitter.convert(" Name  Sunnyvale CA");
            // Expecting exception
            Assert.fail("' Name  Sunnyvale CA' is not a valid name value pair");
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
}
