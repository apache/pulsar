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
package org.apache.pulsar.functions.metrics.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.metrics.MetricsSink;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Utils;

/**
 * A metrics sink that writes to a file  in json format
 * We would create/overwrite a file every time the flush() in invoked
 * We would save at most fileMaximum metrics file in disk
 */
public class FileSink implements MetricsSink {
    private static final Logger LOG = Logger.getLogger(FileSink.class.getName());

    private static final String FILENAME_KEY = "filename-output";
    private static final String MAXIMUM_FILE_COUNT_KEY = "file-maximum";

    // We would convert a file's metrics into a JSON object, i.e. array
    // So we need to add "[" at the start and "]" at the end
    private boolean isFileStart = true;
    private PrintStream writer;
    private String filenameKey;
    private int fileMaximum = 1;
    private int currentFileIndex = 0;

    @Override
    public void init(Map<String, String> conf) {
        verifyConf(conf);
        filenameKey = conf.get(FILENAME_KEY);
        fileMaximum = Integer.valueOf(conf.get(MAXIMUM_FILE_COUNT_KEY));
    }

    private void verifyConf(Map<String, String> conf) {
        if (!conf.containsKey(FILENAME_KEY)) {
            throw new IllegalArgumentException("Require: " + FILENAME_KEY);
        }
        if (!conf.containsKey(MAXIMUM_FILE_COUNT_KEY)) {
            throw new IllegalArgumentException("Require: " + MAXIMUM_FILE_COUNT_KEY);
        }
    }

    private PrintStream openNewFile(String filename) {
        // If the file already exists, set it Writable to avoid permission denied
        File f = new File(filename);
        if (f.exists() && !f.isDirectory()) {
            f.setWritable(true);
        }

        try {
            return new PrintStream(new FileOutputStream(filename, false), true, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new RuntimeException("Error creating " + filename, e);
        }
    }

    @Override
    public void processRecord(InstanceCommunication.MetricsData record, Function.FunctionDetails FunctionDetails) {
        if (isFileStart) {
            String filenamePrefix = filenameKey + "." + FunctionDetailsUtils.getFullyQualifiedName(FunctionDetails);
            writer = openNewFile(String.format("%s.%d", filenamePrefix, currentFileIndex));

            writer.print("[");
            isFileStart = false;
        } else {
            writer.print(",");
        }

        try {
            String metrics = Utils.printJson(record);
            writer.print(metrics);
        } catch (Exception ex) {
        }
    }

    @Override
    public void flush() {
        if (isFileStart) {
            // No record has been processed since the previous flush, so create a new file
            // and output an empty JSON array.
            writer = openNewFile(String.format("%s.%d", filenameKey, currentFileIndex));
            writer.print("[");
        }
        writer.print("]");
        writer.flush();
        writer.close();
        new File(String.format("%s.%s", filenameKey, currentFileIndex)).setReadOnly();

        currentFileIndex = (currentFileIndex + 1) % fileMaximum;

        isFileStart = true;
    }

    @Override
    public void close() {
        if (writer != null) {
            writer.close();
        }
    }
}