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
package org.apache.pulsar.client.admin.internal;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.client.WebTarget;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class WebTargets {

    static WebTarget addParts(WebTarget target, String[] parts) {
        if (parts != null && parts.length > 0) {
            for (String part : parts) {
                String encode;
                try {
                    encode = URLEncoder.encode(part, StandardCharsets.UTF_8.toString());
                } catch (UnsupportedEncodingException e) {
                    log.error("{} is Unknown exception - [{}]", StandardCharsets.UTF_8.toString(), e);
                    encode = part;
                }
                target = target.path(encode);
            }
        }
        return target;
    }

    private WebTargets() {}
}
