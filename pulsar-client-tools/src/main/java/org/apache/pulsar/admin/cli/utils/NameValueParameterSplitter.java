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
package org.apache.pulsar.admin.cli.utils;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public class NameValueParameterSplitter implements IStringConverter<Map<String, String>> {

    @Override
    public Map<String, String> convert(String value) {
        boolean error = false;
        Map<String, String> map = new HashMap<String, String>();

        String[] nvpairs = value.split(",");

        for (String nvpair : nvpairs) {
            error = true;
            if (nvpair != null) {
                String[] nv = nvpair.split("=");
                if (nv != null && nv.length == 2) {
                    nv[0] = nv[0].trim();
                    nv[1] = nv[1].trim();
                    if (!nv[0].isEmpty() && !nv[1].isEmpty() && nv[0].charAt(0) != '\'') {
                        map.put(nv[0], nv[1]);
                        error = false;
                    }
                }
            }

            if (error) {
                break;
            }
        }

        if (error) {
            throw new ParameterException("unable to parse bad name=value parameter list: " + value);
        }

        return map;
    }

}
