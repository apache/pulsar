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

package org.apache.pulsar.functions.utils;

import org.apache.pulsar.functions.proto.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionMetaDataUtils}.
 */
public class FunctionMetaDataUtilsTest {

    @Test
    public void testCanChangeState() {

        long version = 5;
        Function.FunctionMetaData metaData = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-1").setParallelism(2)).setVersion(version).build();

        Assert.assertTrue(FunctionMetaDataUtils.canChangeState(metaData, 0, Function.FunctionState.STOPPED));
        Assert.assertFalse(FunctionMetaDataUtils.canChangeState(metaData, 0, Function.FunctionState.RUNNING));
        Assert.assertFalse(FunctionMetaDataUtils.canChangeState(metaData, 2, Function.FunctionState.STOPPED));
        Assert.assertFalse(FunctionMetaDataUtils.canChangeState(metaData, 2, Function.FunctionState.RUNNING));
    }

    @Test
    public void testChangeState() {
        long version = 5;
        Function.FunctionMetaData metaData = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-1").setParallelism(2)).setVersion(version).build();
        Function.FunctionMetaData newMetaData = FunctionMetaDataUtils.changeFunctionInstanceStatus(metaData, 0, false);
        Assert.assertTrue(newMetaData.getInstanceStatesMap() != null);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().size(), 2);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(0), Function.FunctionState.STOPPED);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(1), Function.FunctionState.RUNNING);
        Assert.assertEquals(newMetaData.getVersion(), version + 1);

        // Nothing should happen
        newMetaData = FunctionMetaDataUtils.changeFunctionInstanceStatus(newMetaData, 3, false);
        Assert.assertTrue(newMetaData.getInstanceStatesMap() != null);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().size(), 2);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(0), Function.FunctionState.STOPPED);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(1), Function.FunctionState.RUNNING);
        Assert.assertEquals(newMetaData.getVersion(), version + 2);

        // Change one more
        newMetaData = FunctionMetaDataUtils.changeFunctionInstanceStatus(newMetaData, 1, false);
        Assert.assertTrue(newMetaData.getInstanceStatesMap() != null);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().size(), 2);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(0), Function.FunctionState.STOPPED);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(1), Function.FunctionState.STOPPED);
        Assert.assertEquals(newMetaData.getVersion(), version + 3);

        // Change all more
        newMetaData = FunctionMetaDataUtils.changeFunctionInstanceStatus(newMetaData, -1, true);
        Assert.assertTrue(newMetaData.getInstanceStatesMap() != null);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().size(), 2);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(0), Function.FunctionState.RUNNING);
        Assert.assertEquals(newMetaData.getInstanceStatesMap().get(1), Function.FunctionState.RUNNING);
        Assert.assertEquals(newMetaData.getVersion(), version + 4);
    }

    @Test
    public void testUpdate() {
        long version = 5;
        Function.FunctionMetaData existingMetaData = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-1").setParallelism(2)).setVersion(version).build();
        Function.FunctionMetaData updatedMetaData = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-1").setParallelism(3)).setVersion(version).build();
        Function.FunctionMetaData newMetaData = FunctionMetaDataUtils.incrMetadataVersion(existingMetaData, updatedMetaData);
        Assert.assertEquals(newMetaData.getVersion(), version + 1);
        Assert.assertEquals(newMetaData.getFunctionDetails().getParallelism(), 3);

        newMetaData = FunctionMetaDataUtils.incrMetadataVersion(null, newMetaData);
        Assert.assertEquals(newMetaData.getVersion(), 0);
    }
}
