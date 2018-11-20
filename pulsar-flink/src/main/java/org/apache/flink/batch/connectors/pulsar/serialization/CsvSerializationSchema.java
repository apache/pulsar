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
package org.apache.flink.batch.connectors.pulsar.serialization;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvSerializationSchema<T> implements SerializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvSerializationSchema.class);
    private static final long serialVersionUID = -3379119592495232636L;

    private static final int STRING_WRITER_INITIAL_BUFFER_SIZE = 256;
    private List<String> fieldNames;
    private PropertyUtils propertyUtils;

    public CsvSerializationSchema(List<String> fieldNames) {
        this.fieldNames = fieldNames;
        propertyUtils = new PropertyUtils();
    }

    @Override
    public byte[] serialize(T t) {
        StringWriter stringWriter = null;
        try {
            Map map = propertyUtils.describe(t);
            if(fieldNames.size() > map.size()) {
                throw new RuntimeException("fieldNames size can not be bigger than model property size");
            }

            List<String> fieldsValues = new ArrayList<>(fieldNames.size());
            for (String fieldName : fieldNames) {
                Object obj = map.getOrDefault(fieldName.toLowerCase(), "");
                fieldsValues.add(obj.toString());
            }

            stringWriter = new StringWriter(STRING_WRITER_INITIAL_BUFFER_SIZE);
            CSVFormat.DEFAULT.printRecord(stringWriter, fieldsValues);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | IOException e) {
            LOG.error("Error while serializing the record to Csv : ", e);
        }

        return stringWriter.toString().getBytes();
    }

}
