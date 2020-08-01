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
package org.apache.pulsar.functions.instance;

import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class InstanceUtilsTest {

    /**
     * Test the calculateSubjectType function for sources
     */
    @Test
    public void testCalculateSubjectTypeForSource() {
        FunctionDetails.Builder builder = FunctionDetails.newBuilder();
        // no input topics mean source
        builder.setSource(Function.SourceSpec.newBuilder().build());
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.SOURCE);
        // make sure that if the componenttype is set, that gets precedence.
        builder.setComponentType(FunctionDetails.ComponentType.SINK);
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.SINK);
        builder.setComponentType(FunctionDetails.ComponentType.FUNCTION);
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.FUNCTION);
    }

    /**
     * Test the calculateSubjectType function for function
     */
    @Test
    public void testCalculateSubjectTypeForFunction() {
        FunctionDetails.Builder builder = FunctionDetails.newBuilder();
        // an input but no sink classname is a function
        builder.setSource(Function.SourceSpec.newBuilder().putInputSpecs("topic", Function.ConsumerSpec.newBuilder().build()).build());
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.FUNCTION);
        // make sure that if the componenttype is set, that gets precedence.
        builder.setComponentType(FunctionDetails.ComponentType.SOURCE);
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.SOURCE);
        builder.setComponentType(FunctionDetails.ComponentType.SINK);
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.SINK);
    }

    /**
     * Test the calculateSubjectType function for Sink
     */
    @Test
    public void testCalculateSubjectTypeForSink() {
        FunctionDetails.Builder builder = FunctionDetails.newBuilder();
        // an input and a sink classname is a sink
        builder.setSource(Function.SourceSpec.newBuilder().putInputSpecs("topic", Function.ConsumerSpec.newBuilder().build()).build());
        builder.setSink(Function.SinkSpec.newBuilder().setClassName("something").build());
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.SINK);
        // make sure that if the componenttype is set, that gets precedence.
        builder.setComponentType(FunctionDetails.ComponentType.SOURCE);
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.SOURCE);
        builder.setComponentType(FunctionDetails.ComponentType.FUNCTION);
        assertEquals(InstanceUtils.calculateSubjectType(builder.build()), FunctionDetails.ComponentType.FUNCTION);
    }
}
