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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Maps;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.Metrics;

public class MBeanStatsGenerator {
    private MBeanServer mbs;

    public static Collection<Metrics> generate(PulsarService pulsar) {
        return new MBeanStatsGenerator(pulsar).generate();
    }

    // hide
    MBeanStatsGenerator(PulsarService pulsar) {
        this.mbs = ManagementFactory.getPlatformMBeanServer();
    }

    private Collection<Metrics> generate() {
        List<Metrics> metricsCollection = new ArrayList<Metrics>();

        @SuppressWarnings("unchecked")
        Set<ObjectInstance> instances = mbs.queryMBeans(null, null);

        for (ObjectInstance instance : instances) {
            String beanName = instance.getObjectName().toString();

            // skip GC MBean to avoid recursion
            if (beanName.startsWith("java.lang:type=GarbageCollector")) {
                continue;
            }

            Metrics metrics = convert(instance);
            if (metrics != null) {
                metricsCollection.add(metrics);
            }
        }

        return metricsCollection;
    }

    private Metrics convert(ObjectInstance instance) {

        ObjectName objName = instance.getObjectName();

        MBeanInfo info = null;

        try {
            info = mbs.getMBeanInfo(objName);
        } catch (Exception e) {
            // [Bug 6158364] skipping MBean if access failed
            return null;
        }

        Metrics metrics = null;

        // create a metrics instance by MBean dimension
        metrics = createMetricsByDimension(objName);

        // get each of attribute,value from the MBean
        for (MBeanAttributeInfo attr : info.getAttributes()) {
            Object value;

            try {
                value = mbs.getAttribute(instance.getObjectName(), attr.getName());
                metrics.put(attr.getName(), value);
            } catch (Exception e) {
                // skip
            }
        }

        return metrics;
    }

    /**
     * Creates a MBean dimension key for metrics.
     *
     * @param objectName
     * @return
     */
    private Metrics createMetricsByDimension(ObjectName objectName) {
        Map<String, String> dimensionMap = Maps.newHashMap();

        dimensionMap.put("MBean", objectName.toString());

        // create with current version
        return Metrics.create(dimensionMap);
    }
}
