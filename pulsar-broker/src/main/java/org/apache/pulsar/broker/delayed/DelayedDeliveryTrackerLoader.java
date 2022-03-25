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
package org.apache.pulsar.broker.delayed;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.broker.ServiceConfiguration;

@UtilityClass
public class DelayedDeliveryTrackerLoader {
    public static DelayedDeliveryTrackerFactory loadDelayedDeliveryTrackerFactory(ServiceConfiguration conf)
            throws IOException {
        Class<?> factoryClass;
        try {
            factoryClass = Class.forName(conf.getDelayedDeliveryTrackerFactoryClassName());
            Object obj = factoryClass.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof DelayedDeliveryTrackerFactory,
                    "The factory has to be an instance of " + DelayedDeliveryTrackerFactory.class.getName());

            DelayedDeliveryTrackerFactory factory = (DelayedDeliveryTrackerFactory) obj;
            factory.initialize(conf);
            return factory;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
