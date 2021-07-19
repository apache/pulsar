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
package org.apache.pulsar.admin.cli;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

abstract class CliCommand {

    static String[] validatePropertyCluster(List<String> params) {
        return splitParameter(params, 2);
    }

    static String validateNamespace(List<String> params) {
        String namespace = checkArgument(params);
        return NamespaceName.get(namespace).toString();
    }

    static String validateTopicName(List<String> params) {
        String topic = checkArgument(params);
        return TopicName.get(topic).toString();
    }

    static String validatePersistentTopic(List<String> params) {
        String topic = checkArgument(params);
        TopicName topicName = TopicName.get(topic);
        if (topicName.getDomain() != TopicDomain.persistent) {
            throw new ParameterException("Need to provide a persistent topic name");
        }
        return topicName.toString();
    }

    static String validateNonPersistentTopic(List<String> params) {
        String topic = checkArgument(params);
        TopicName topicName = TopicName.get(topic);
        if (topicName.getDomain() != TopicDomain.non_persistent) {
            throw new ParameterException("Need to provide a non-persistent topic name");
        }
        return topicName.toString();
    }

    static void validateLatencySampleRate(int sampleRate) {
        if (sampleRate < 0) {
            throw new ParameterException(
                    "Latency sample rate should be positive and non-zero (found " + sampleRate + ")");
        }
    }

    static long validateSizeString(String s) {
        char last = s.charAt(s.length() - 1);
        String subStr = s.substring(0, s.length() - 1);
        switch (last) {
        case 'k':
        case 'K':
            return Long.parseLong(subStr) * 1024;

        case 'm':
        case 'M':
            return Long.parseLong(subStr) * 1024 * 1024;

        case 'g':
        case 'G':
            return Long.parseLong(subStr) * 1024 * 1024 * 1024;

        case 't':
        case 'T':
            return Long.parseLong(subStr) * 1024 * 1024 * 1024 * 1024;

        default:
            return Long.parseLong(s);
        }
    }

    static MessageId validateMessageIdString(String resetMessageIdStr) throws PulsarAdminException {
        return validateMessageIdString(resetMessageIdStr, -1);
    }

    static MessageId validateMessageIdString(String resetMessageIdStr, int partitionIndex) throws PulsarAdminException {
        String[] messageId = resetMessageIdStr.split(":");
        try {
            Preconditions.checkArgument(messageId.length == 2);
            return new MessageIdImpl(Long.parseLong(messageId[0]), Long.parseLong(messageId[1]), partitionIndex);
        } catch (Exception e) {
            throw new PulsarAdminException(
                    "Invalid message id (must be in format: ledgerId:entryId) value " + resetMessageIdStr);
        }
    }

    static String checkArgument(List<String> arguments) {
        if (arguments.size() != 1) {
            throw new ParameterException("Need to provide just 1 parameter");
        }

        return arguments.get(0);
    }

    private static String[] splitParameter(List<String> params, int n) {
        if (params.size() != 1) {
            throw new ParameterException("Need to provide just 1 parameter");
        }

        String[] parts = params.get(0).split("/");
        if (parts.length != n) {
            throw new ParameterException("Paramter format is incorrect");
        }

        return parts;
    }

    static String getOneArgument(List<String> params) {
        if (params.size() != 1) {
            throw new ParameterException("Need to provide just 1 parameter");
        }

        return params.get(0);
    }

    /**
     *
     * @param params
     *            List of positional arguments
     * @param pos
     *            Positional arguments start with index as 1
     * @param maxArguments
     *            Validate against max arguments
     * @return
     */
    static String getOneArgument(List<String> params, int pos, int maxArguments) {
        if (params.size() != maxArguments) {
            throw new ParameterException(String.format("Need to provide %s parameters", maxArguments));
        }

        return params.get(pos);
    }

    static Set<AuthAction> getAuthActions(List<String> actions) {
        Set<AuthAction> res = Sets.newTreeSet();
        for (String action : actions) {
            res.add(AuthAction.valueOf(action));
        }

        return res;
    }

    <T> void print(List<T> items) {
        for (T item : items) {
            print(item);
        }
    }

    <K,V> void print(Map<K,V> items) {
        for(Map.Entry<K,V> entry : items.entrySet()) {
            print(entry.getKey() + "    " + entry.getValue());
        }
    }

    <T> void print(T item) {
        try {
            System.out.println(writer.writeValueAsString(item));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ObjectMapper mapper = ObjectMapperFactory.create();
    private static ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();

    abstract void run() throws Exception;
}
