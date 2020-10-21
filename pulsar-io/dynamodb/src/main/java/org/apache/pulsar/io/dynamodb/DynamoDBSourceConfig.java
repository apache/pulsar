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
package org.apache.pulsar.io.dynamodb;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Date;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import software.amazon.awssdk.regions.Region;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.Data;

@Data
public class DynamoDBSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Dynamodb streams end-point url. It can be found at https://docs.aws.amazon.com/general/latest/gr/rande.html"
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
            help = "Dynamodb stream arn"
    )
    private String awsDynamodbStreamArn = "";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Fully-Qualified class name of implementation of AwsCredentialProviderPlugin."
                    + " It is a factory class which creates an AWSCredentialsProvider that will be used by dynamodb."
                    + " If it is empty then dynamodb will create a default AWSCredentialsProvider which accepts json-map"
                    + " of credentials in `awsCredentialPluginParam`")
    private String awsCredentialPluginName = "";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "json-parameters to initialize `AwsCredentialsProviderPlugin`")
    private String awsCredentialPluginParam = "";

    @FieldDoc(
        required = false,
        defaultValue = "LATEST",
        help = "Used to specify the position in the stream where the connector should start from.\n"
                + "  #\n"
                + "  # The available options are: \n"
                + "  #\n"
                + "  # - AT_TIMESTAMP \n"
                + "  #\n"
                + "  #   Start from the record at or after the specified timestamp. \n"
                + "  #\n"
                + "  # - LATEST \n"
                + "  #\n"
                + "  #   Start after the most recent data record (fetch new data). \n"
                + "  #\n"
                + "  # - TRIM_HORIZON \n"
                + "  #\n"
                + "  #   Start from the oldest available data record. \n"
    )
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "If the initalPositionInStream is set to 'AT_TIMESTAMP', then this "
                + " property specifies the point in time to start consumption."
    )
    private Date startAtTime;

    @FieldDoc(
        required = false,
        defaultValue = "Apache Pulsar IO Connector",
        help = "Name of the dynamodb consumer application. By default the application name is included "
                + "in the user agent string used to make AWS requests. This can assist with troubleshooting "
                + "(e.g. distinguish requests made by separate connectors instances)."
    )
    private String applicationName = "pulsar-dynamodb";

    @FieldDoc(
        required = false,
        defaultValue = "60000",
        help = "The frequency of the stream checkpointing (in milliseconds)"
    )
    private long checkpointInterval = 60000L;

    @FieldDoc(
        required = false,
        defaultValue = "3000",
        help = "The amount of time to delay between requests when the connector encounters a Throttling"
                + "exception from dynamodb (in milliseconds)"
    )
    private long backoffTime = 3000L;

    @FieldDoc(
        required = false,
        defaultValue = "3",
        help = "The number of re-attempts to make when the connector encounters an "
                + "exception while trying to set a checkpoint"
    )
    private int numRetries = 3;

    @FieldDoc(
        required = false,
        defaultValue = "1000",
        help = "The maximum number of AWS Records that can be buffered inside the connector. "
                + "Once this is reached, the connector will not consume any more messages from "
                + "Kinesis until some of the messages in the queue have been successfully consumed."
    )
    private int receiveQueueSize = 1000;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Dynamo end-point url. It can be found at https://docs.aws.amazon.com/general/latest/gr/rande.html"
    )
    private String dynamoEndpoint = "";

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Cloudwatch end-point url. It can be found at https://docs.aws.amazon.com/general/latest/gr/rande.html"
    )
    private String cloudwatchEndpoint = "";


    public static DynamoDBSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), DynamoDBSourceConfig.class);
    }

    public static DynamoDBSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), DynamoDBSourceConfig.class);
    }

    protected Region regionAsV2Region() {
        return Region.of(this.getAwsRegion());
    }

    public AmazonDynamoDBStreams buildDynamoDBStreamsClient(AwsCredentialProviderPlugin credPlugin) {
        AmazonDynamoDBStreamsClientBuilder builder = AmazonDynamoDBStreamsClientBuilder.standard();

        if (!this.getAwsEndpoint().isEmpty()) {
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(this.getAwsEndpoint(), this.getAwsRegion()));
        }
        if (!this.getAwsRegion().isEmpty()) {
            builder.setRegion(this.getAwsRegion());
        }
        builder.setCredentials(credPlugin.getCredentialProvider());
        return builder.build();
    }

    public AmazonDynamoDB buildDynamoDBClient(AwsCredentialProviderPlugin credPlugin) {
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();

        if (!this.getAwsEndpoint().isEmpty()) {
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(this.getDynamoEndpoint(), this.getAwsRegion()));
        }
        if (!this.getAwsRegion().isEmpty()) {
            builder.setRegion(this.getAwsRegion());
        }
        builder.setCredentials(credPlugin.getCredentialProvider());
        return builder.build();
    }

    public AmazonCloudWatch buildCloudwatchClient(AwsCredentialProviderPlugin credPlugin) {
        AmazonCloudWatchClientBuilder builder = AmazonCloudWatchClientBuilder.standard();

        if (!this.getAwsEndpoint().isEmpty()) {
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(this.getCloudwatchEndpoint(), this.getAwsRegion()));
        }
        if (!this.getAwsRegion().isEmpty()) {
            builder.setRegion(this.getAwsRegion());
        }
        builder.setCredentials(credPlugin.getCredentialProvider());
        return builder.build();
    }

    public InitialPositionInStreamExtended getStreamStartPosition() {
        if (initialPositionInStream == InitialPositionInStream.AT_TIMESTAMP) {
            return InitialPositionInStreamExtended.newInitialPositionAtTimestamp(getStartAtTime());
        }
        else {
            return InitialPositionInStreamExtended.newInitialPosition(this.getInitialPositionInStream());
        }
    }
}
