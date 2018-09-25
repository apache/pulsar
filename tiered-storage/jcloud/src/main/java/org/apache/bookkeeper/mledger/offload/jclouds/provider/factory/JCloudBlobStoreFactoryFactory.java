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
package org.apache.bookkeeper.mledger.offload.jclouds.provider.factory;

import static org.apache.pulsar.common.util.FieldParser.value;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating JCloud Blob Store Configurations.
 */
public class JCloudBlobStoreFactoryFactory implements OffloadDriverMetadataKeys {

    private static final Logger log = LoggerFactory.getLogger(JCloudBlobStoreFactoryFactory.class);
    public static final String BLOB_STORE_PROVIDER_KEY = "managedLedgerOffloadDriver";

    public static JCloudBlobStoreFactory create(Map<String, String> offloadDriverMetadata) throws IOException {

        if (offloadDriverMetadata == null || offloadDriverMetadata.isEmpty()) {
            throw new IOException("offloadDriverMetadata must not be empty");
        }

        Properties props = new Properties();
        props.putAll(offloadDriverMetadata);
        props.put(JCloudBlobStoreFactoryFactory.BLOB_STORE_PROVIDER_KEY,
                offloadDriverMetadata.get(METADATA_FIELD_BLOB_STORE_PROVIDER));

        return JCloudBlobStoreFactoryFactory.create(props);
    }

    public static JCloudBlobStoreFactory create(Properties props) throws IOException {
        return create(props, true);
    }

    public static JCloudBlobStoreFactory create(Properties props, boolean validate) throws IOException {

       String driver = props.getProperty(BLOB_STORE_PROVIDER_KEY, "AWS_S3");
       JCloudBlobStoreProvider provider = JCloudBlobStoreProvider.valueOf(driver.toUpperCase());
       try {
           if (provider == null) {
               throw new IOException(
                       "Not support this kind of driver as offload backend: " + driver);
           }
           JCloudBlobStoreFactory factory = JCloudBlobStoreFactoryFactory.create(provider.getClazz(), props);
           factory.setProvider(provider);

           if (validate) {
              factory.validate();
           }

           return factory;
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
    private static JCloudBlobStoreFactory create(Class<? extends JCloudBlobStoreFactory> clazz,
            Properties properties) throws InstantiationException, IllegalAccessException {

        JCloudBlobStoreFactory data = clazz.newInstance();

        if (properties != null && !properties.isEmpty()) {
            Collection<Field> allFields = getFields(clazz);
            Field[] fields = allFields.toArray(new Field[allFields.size()]);

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
        }
        return data;
    }

    private static Collection<Field> getFields(Class<?> clazz) {
        Map<String, Field> fields = new HashMap<String, Field>();
        while (clazz != null) {
          for (Field field : clazz.getDeclaredFields()) {
            if (!fields.containsKey(field.getName())) {
              fields.put(field.getName(), field);
            }
          }

          clazz = clazz.getSuperclass();
        }

        return fields.values();
    }
}