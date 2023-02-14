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
package org.apache.pulsar.utils;

import com.google.common.io.Resources;
import java.io.File;

public class ResourceUtils {

    public static String getAbsolutePath(String resourceName) {
        // On Windows, URL#getPath might return a string that starts with a disk name, e.g. "/C:/"
        // It's invalid to use this path to open a file, so we need to get the absolute path via File.
        return new File(Resources.getResource(resourceName).getPath()).getAbsolutePath();
    }
}
