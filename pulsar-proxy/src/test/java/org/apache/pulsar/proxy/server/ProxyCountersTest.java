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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.model.snapshots.Labels;
import java.util.List;
import org.testng.annotations.Test;

public class ProxyCountersTest {

    /**
     * The proxy does not export exemplars, and two of these counters are incremented for every proxied frame,
     * so none of them should keep an exemplar sampler.
     */
    @Test
    public void testCountersDoNotSampleExemplars() {
        Labels exemplarLabels = Labels.of("trace_id", "0123456789abcdef");
        for (Counter counter : List.of(ProxyService.NEW_CONNECTIONS, ProxyService.REJECTED_CONNECTIONS,
                ProxyService.OPS_COUNTER, ProxyService.BYTES_COUNTER, LookupProxyHandler.REJECTED_LOOKUP_REQUESTS,
                LookupProxyHandler.REJECTED_PARTITIONS_METADATA_REQUESTS,
                LookupProxyHandler.REJECTED_GET_TOPICS_OF_NAMESPACE_REQUESTS)) {
            // an increment of 0 leaves the value of the shared counter alone
            counter.incWithExemplar(0, exemplarLabels);
            assertThat(counter.collect().getDataPoints().get(0).getExemplar())
                    .as(counter.getPrometheusName()).isNull();
        }
    }
}
