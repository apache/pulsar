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
package org.apache.pulsar.io.elasticsearch;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class ElasticSearchSslConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "Enable SSL/TLS"
    )
    private boolean enabled = false;

    @FieldDoc(
            required = false,
            defaultValue = "None",
            help = "SSL Provider"
    )
    private String provider;

    @FieldDoc(
            required = false,
            defaultValue = "true",
            help = "Whether or not to validate node hostnames when using SSL"
    )
    private boolean hostnameVerification = true;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The path to the truststore file"
    )
    private String truststorePath;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Truststore password"
    )
    private String truststorePassword;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The path to the keystore file"
    )
    private String keystorePath;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Keystore password"
    )
    private String keystorePassword;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "SSL/TLS cipher suites"
    )
    private String cipherSuites;

    @FieldDoc(
            required = false,
            defaultValue = "TLSv1.2",
            help = "Comma separated list of enabled SSL/TLS protocols"
    )
    private String protocols = "TLSv1.2";

}
