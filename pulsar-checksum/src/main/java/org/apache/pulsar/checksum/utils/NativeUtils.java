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
package org.apache.pulsar.checksum.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

import static com.google.common.base.Preconditions.*;

public class NativeUtils {

    public static final String OS_NAME = System.getProperty("os.name").toLowerCase(Locale.US);

    /**
     * loads given library from the this jar. ie: this jar contains: /lib/pulsar-checksum.jnilib
     * 
     * @param path
     *            : absolute path of the library in the jar <br/>
     *            if this jar contains: /lib/pulsar-checksum.jnilib then provide the same absolute path as input
     * @throws Exception
     */
    public static void loadLibraryFromJar(String path) throws Exception {

        checkArgument(path.startsWith("/"), "absolute path must start with  /");

        String[] parts = path.split("/");
        String filename = (parts.length > 0) ? parts[parts.length - 1] : null;

        File dir = File.createTempFile("native", "");
        dir.delete();
        if (!(dir.mkdir())) {
            throw new IOException("Failed to create temp directory " + dir.getAbsolutePath());
        }
        dir.deleteOnExit();
        File temp = new File(dir, filename);
        temp.deleteOnExit();

        byte[] buffer = new byte[1024];
        int read;

        InputStream input = NativeUtils.class.getResourceAsStream(path);
        if (input == null) {
            throw new FileNotFoundException("Couldn't find file into jar " + path);
        }

        OutputStream out = new FileOutputStream(temp);
        try {
            while ((read = input.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
        } finally {
            out.close();
            input.close();
        }

        if (!temp.exists()) {
            throw new FileNotFoundException("Failed to copy file from jar at " + temp.getAbsolutePath());
        }

        System.load(temp.getAbsolutePath());
    }

    /**
     * Returns jni library extension based on OS specification. Maven-nar generates jni library based on different OS :
     * http://mark.donszelmann.org/maven-nar-plugin/aol.html (jni.extension)
     * 
     * @return
     */
    public static String libType() {

        if (OS_NAME.indexOf("mac") >= 0) {
            return "jnilib";
        } else if (OS_NAME.indexOf("nix") >= 0 || OS_NAME.indexOf("nux") >= 0 || OS_NAME.indexOf("aix") > 0) {
            return "so";
        } else if (OS_NAME.indexOf("win") >= 0) {
            return "dll";
        }
        throw new TypeNotPresentException(OS_NAME + " not supported", null);
    }
}
