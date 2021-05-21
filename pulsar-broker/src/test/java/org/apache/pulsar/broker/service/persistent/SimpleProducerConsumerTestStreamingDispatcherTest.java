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
package org.apache.pulsar.broker.service.persistent;

import org.apache.pulsar.broker.service.streamingdispatch.StreamingDispatcher;
import org.apache.pulsar.client.api.SimpleProducerConsumerTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * SimpleProducerConsumerTest with {@link StreamingDispatcher}
 */
@Test(groups = "flaky")
public class SimpleProducerConsumerTestStreamingDispatcherTest extends SimpleProducerConsumerTest {

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setStreamingDispatch(true);
    }
}
