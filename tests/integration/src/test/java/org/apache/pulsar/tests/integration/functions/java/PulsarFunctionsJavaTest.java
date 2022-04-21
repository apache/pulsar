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
package org.apache.pulsar.tests.integration.functions.java;

import static org.testng.Assert.assertEquals;

import java.util.Collections;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.FunctionStatusUtil;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTest;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.Test;

public class PulsarFunctionsJavaTest extends PulsarFunctionsTest {

	PulsarFunctionsJavaTest(FunctionRuntimeType functionRuntimeType) {
		super(functionRuntimeType);
	}

   @Test(groups = {"java_function", "function"})
    public void testJavaFunctionLocalRun() throws Exception {
        testFunctionLocalRun(Runtime.JAVA);
    }

   @Test(groups = {"java_function", "function"})
   public void testJavaFunctionNegAck() throws Exception {
       testFunctionNegAck(Runtime.JAVA);
   }

   @Test(groups = {"java_function", "function"})
   public void testJavaPublishFunction() throws Exception {
       testPublishFunction(Runtime.JAVA);
   }

   @Test(groups = {"java_function", "function"})
   public void testSerdeFunction() throws Exception {
       testCustomSerdeFunction();
   }

   private void testCustomSerdeFunction() throws Exception {
       if (functionRuntimeType == FunctionRuntimeType.THREAD) {
           return;
       }

       String inputTopicName = "persistent://public/default/test-serde-java-input-" + randomName(8);
       String outputTopicName = "test-publish-serde-output-" + randomName(8);
       try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
           admin.topics().createNonPartitionedTopic(inputTopicName);
           admin.topics().createNonPartitionedTopic(outputTopicName);
       }

       String functionName = "test-serde-fn-" + randomName(8);
       submitFunction(
               Runtime.JAVA, inputTopicName, outputTopicName, functionName, null, SERDE_JAVA_CLASS,
               SERDE_OUTPUT_CLASS, Collections.singletonMap("serde-topic", outputTopicName)
       );

       // get function info
       getFunctionInfoSuccess(functionName);
       // get function stats
       getFunctionStatsEmpty(functionName);

       ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
               PulsarCluster.ADMIN_SCRIPT,
               "functions",
               "status",
               "--tenant", "public",
               "--namespace", "default",
               "--name", functionName
       );

       FunctionStatus functionStatus = FunctionStatusUtil.decode(result.getStdout());
       assertEquals(functionStatus.getNumInstances(), 1);
       assertEquals(functionStatus.getInstances().get(0).getStatus().isRunning(), true);
   }


   @Test(groups = {"java_function", "function"})
   public void testJavaExclamationFunction() throws Exception {
       testExclamationFunction(Runtime.JAVA, false, false, false);
   }

   @Test(groups = {"java_function", "function"})
   public void testJavaExclamationTopicPatternFunction() throws Exception {
       testExclamationFunction(Runtime.JAVA, true, false, false);
   }

   @Test(groups = {"java_function", "function"})
   public void testJavaLoggingFunction() throws Exception {
       testLoggingFunction(Runtime.JAVA);
   }


   @Test(groups = {"java_function", "function"})
   public void testInitFunction() throws Exception {
	   testInitFunction(Runtime.JAVA);
   }

   @Test(groups = {"java_function", "function"})
   public void testTumblingCountWindowTest() throws Exception {
       String[] EXPECTED_RESULTS = {
               "0,1,2,3,4,5,6,7,8,9",
               "10,11,12,13,14,15,16,17,18,19",
               "20,21,22,23,24,25,26,27,28,29",
               "30,31,32,33,34,35,36,37,38,39",
               "40,41,42,43,44,45,46,47,48,49",
               "50,51,52,53,54,55,56,57,58,59",
               "60,61,62,63,64,65,66,67,68,69",
               "70,71,72,73,74,75,76,77,78,79",
               "80,81,82,83,84,85,86,87,88,89",
               "90,91,92,93,94,95,96,97,98,99",
       };

       testWindowFunction("tumbling", EXPECTED_RESULTS);
   }

   @Test(groups = {"java_function", "function"})
   public void testSlidingCountWindowTest() throws Exception {
       String[] EXPECTED_RESULTS = {
               "0,1,2,3,4",
               "0,1,2,3,4,5,6,7,8,9",
               "5,6,7,8,9,10,11,12,13,14",
               "10,11,12,13,14,15,16,17,18,19",
               "15,16,17,18,19,20,21,22,23,24",
               "20,21,22,23,24,25,26,27,28,29",
               "25,26,27,28,29,30,31,32,33,34",
               "30,31,32,33,34,35,36,37,38,39",
               "35,36,37,38,39,40,41,42,43,44",
               "40,41,42,43,44,45,46,47,48,49",
               "45,46,47,48,49,50,51,52,53,54",
               "50,51,52,53,54,55,56,57,58,59",
               "55,56,57,58,59,60,61,62,63,64",
               "60,61,62,63,64,65,66,67,68,69",
               "65,66,67,68,69,70,71,72,73,74",
               "70,71,72,73,74,75,76,77,78,79",
               "75,76,77,78,79,80,81,82,83,84",
               "80,81,82,83,84,85,86,87,88,89",
               "85,86,87,88,89,90,91,92,93,94",
               "90,91,92,93,94,95,96,97,98,99",
       };

       testWindowFunction("sliding", EXPECTED_RESULTS);
   }

    @Test(groups = {"java_function", "function"})
    public void testMergeFunctionTest() throws Exception {
	    testMergeFunction();
   }

    @Test(groups = {"java_function", "function"})
    public void testGenericObjectFunction() throws Exception {
        testGenericObjectFunction(GENERIC_OBJECT_FUNCTION_JAVA_CLASS, false, false);
    }

    @Test(groups = {"java_function", "function"})
    public void testGenericObjectRemoveFiledFunction() throws Exception {
        testGenericObjectFunction(REMOVE_AVRO_FIELD_FUNCTION_JAVA_CLASS, true, false);
    }

    @Test(groups = {"java_function", "function"})
    public void testGenericObjectFunctionKeyValue() throws Exception {
        testGenericObjectFunction(GENERIC_OBJECT_FUNCTION_JAVA_CLASS, false, true);
    }

    @Test(groups = {"java_function", "function"})
    public void testGenericObjectRemoveFiledFunctionKeyValue() throws Exception {
        testGenericObjectFunction(REMOVE_AVRO_FIELD_FUNCTION_JAVA_CLASS, true, true);
    }

}
