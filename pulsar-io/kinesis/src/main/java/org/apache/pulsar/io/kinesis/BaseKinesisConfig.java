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
package org.apache.pulsar.io.kinesis;

import java.io.Serializable;
import lombok.Data;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import software.amazon.awssdk.regions.Region;

@Data
public abstract class BaseKinesisConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Kinesis end-point url. It can be found at https://docs.aws.amazon.com/general/latest/gr/rande.html"
    )
    private String awsEndpoint = "";

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Appropriate aws region. E.g. us-west-1, us-west-2"
    )
    private String awsRegion = "";

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Kinesis stream name"
    )
    private String awsKinesisStreamName = "";

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Fully-Qualified class name of implementation of AwsCredentialProviderPlugin."
            + " It is a factory class which creates an AWSCredentialsProvider that will be used by Kinesis Sink."
            + " If it is empty then KinesisSink will create a default AWSCredentialsProvider which accepts json-map"
            + " of credentials in `awsCredentialPluginParam`")
    private String awsCredentialPluginName = "";

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "json-parameters to initialize `AwsCredentialsProviderPlugin`")
    private String awsCredentialPluginParam = "";

    protected Region regionAsV2Region() {
        return Region.of(this.getAwsRegion());
    }
}
