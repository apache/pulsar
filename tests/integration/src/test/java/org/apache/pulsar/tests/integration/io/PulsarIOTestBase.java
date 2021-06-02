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
package org.apache.pulsar.tests.integration.io;

import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTestBase;
import org.apache.pulsar.tests.integration.io.sinks.PulsarIOSinkRunner;
import org.apache.pulsar.tests.integration.io.sinks.SinkTester;
import org.apache.pulsar.tests.integration.io.sources.PulsarIOSourceRunner;
import org.apache.pulsar.tests.integration.io.sources.SourceTester;
import org.testcontainers.containers.GenericContainer;

public abstract class PulsarIOTestBase extends PulsarFunctionsTestBase {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void testSink(SinkTester tester, boolean builtin) throws Exception {
        tester.startServiceContainer(pulsarCluster);
        try {
        	PulsarIOSinkRunner runner = new PulsarIOSinkRunner(pulsarCluster, functionRuntimeType.toString());
            runner.runSinkTester(tester, builtin);
        } finally {
            tester.stopServiceContainer(pulsarCluster);
        }
    }

	@SuppressWarnings("rawtypes")
	protected <ServiceContainerT extends GenericContainer>  void testSink(SinkTester<ServiceContainerT> sinkTester,
			boolean builtinSink,
			SourceTester<ServiceContainerT> sourceTester) throws Exception {

		ServiceContainerT serviceContainer = sinkTester.startServiceContainer(pulsarCluster);

		try {
			PulsarIOSinkRunner runner = new PulsarIOSinkRunner(pulsarCluster, functionRuntimeType.toString());
            runner.runSinkTester(sinkTester, builtinSink);
			if (null != sourceTester) {
				PulsarIOSourceRunner sourceRunner = new PulsarIOSourceRunner(pulsarCluster, functionRuntimeType.toString());
				sourceTester.setServiceContainer(serviceContainer);
				sourceRunner.testSource(sourceTester);
			}
		} finally {
			sinkTester.stopServiceContainer(pulsarCluster);
		}
    }
}
