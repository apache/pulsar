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
package org.apache.bookkeeper.mledger.offload.jcloud.config;

import static org.apache.pulsar.common.util.FieldParser.value;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating JCloud Blob Store Configurations.
 */
public class JCloudBlobStoreConfigurationFactory {

    private static final Logger log = LoggerFactory.getLogger(JCloudBlobStoreConfigurationFactory.class);

    public static JCloudBlobStoreConfiguration create(Properties props) throws IOException {

       String driver = props.getProperty("managedLedgerOffloadDriver", "AWS");
       JCloudBlobStoreProvider provider = JCloudBlobStoreProvider.valueOf(driver.toUpperCase());
       try {
           if (provider == null) {
               throw new IOException(
                       "Not support this kind of driver as offload backend: " + driver);
           }
           JCloudBlobStoreConfiguration config = JCloudBlobStoreConfigurationFactory.create(provider.getClazz(), props);
           config.setProvider(provider);
           config.validate();
           return config;
       } catch (InstantiationException | IllegalAccessException | IllegalArgumentException e) {
          log.error("Unable to create storage configuration ", e);
          throw new IOException(e);
       }
    }

    /**
     * Create a tiered storage configuration from the provided <tt>properties</tt>.
     *
     * @param properties the configuration properties
     * @return tiered storage configuration
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private static JCloudBlobStoreConfiguration create(Class<? extends JCloudBlobStoreConfiguration> clazz,
            Properties properties) throws InstantiationException, IllegalAccessException {
        JCloudBlobStoreConfiguration data = clazz.newInstance();
        Field[] fields = (Field[]) ArrayUtils.addAll(
                clazz.getSuperclass().getDeclaredFields(),
                clazz.getDeclaredFields());

        Arrays.stream(fields).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    f.set(data, value((String) properties.get(f.getName()), f));
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("failed to initialize %s field while setting value %s",
                            f.getName(), properties.get(f.getName())), e);
                }
            }
        });
        return data;
    }
}