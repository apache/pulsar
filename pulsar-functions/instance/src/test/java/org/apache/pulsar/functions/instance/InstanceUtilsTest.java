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
package org.apache.pulsar.functions.instance;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.testng.annotations.Test;

public class InstanceUtilsTest {

    /**
     * Test the calculateSubjectType function for sources.
     */
    @Test
    public void testCalculateSubjectTypeForSource() {
        FunctionDetails functionDetails = new FunctionDetails();
        // no input topics mean source
        functionDetails.setSource();
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.SOURCE);
        // make sure that if the componenttype is set, that gets precedence.
        functionDetails.setComponentType(FunctionDetails.ComponentType.SINK);
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.SINK);
        functionDetails.setComponentType(FunctionDetails.ComponentType.FUNCTION);
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.FUNCTION);
    }

    /**
     * Test the calculateSubjectType function for function.
     */
    @Test
    public void testCalculateSubjectTypeForFunction() {
        FunctionDetails functionDetails = new FunctionDetails();
        // an input but no sink classname is a function
        functionDetails.setSource().putInputSpecs("topic");
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.FUNCTION);
        // make sure that if the componenttype is set, that gets precedence.
        functionDetails.setComponentType(FunctionDetails.ComponentType.SOURCE);
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.SOURCE);
        functionDetails.setComponentType(FunctionDetails.ComponentType.SINK);
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.SINK);
    }

    /**
     * Test the calculateSubjectType function for Sink.
     */
    @Test
    public void testCalculateSubjectTypeForSink() {
        FunctionDetails functionDetails = new FunctionDetails();
        // an input and a sink classname is a sink
        functionDetails.setSource().putInputSpecs("topic");
        functionDetails.setSink().setClassName("something");
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.SINK);
        // make sure that if the componenttype is set, that gets precedence.
        functionDetails.setComponentType(FunctionDetails.ComponentType.SOURCE);
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.SOURCE);
        functionDetails.setComponentType(FunctionDetails.ComponentType.FUNCTION);
        assertEquals(InstanceUtils.calculateSubjectType(functionDetails), FunctionDetails.ComponentType.FUNCTION);
    }
}
