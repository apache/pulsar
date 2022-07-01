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
package org.apache.pulsar.sql.presto;

import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.prestosql.spi.StandardErrorCode.QUERY_REJECTED;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * This class implements the authentication and authorization integration between the Pulsar SQL worker and the
 * Pulsar broker.
 *
 * It will check permissions against the session-topic pair by trying to subscribe to a topic using the Pulsar Reader
 * to check the consumption privilege. The same topic will only be checked once during the same session.
 */
public class PulsarAuth {

    private static final Logger log = Logger.get(PulsarAuth.class);

    private final PulsarConnectorConfig pulsarConnectorConfig;
    private static final String CREDENTIALS_AUTH_PLUGIN = "auth-plugin";
    private static final String CREDENTIALS_AUTH_PARAMS = "auth-params";
    @VisibleForTesting
    final Map<String, Set<String>> authorizedQueryTopicsMap = new ConcurrentHashMap<>();

    @Inject
    public PulsarAuth(PulsarConnectorConfig pulsarConnectorConfig) {
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        if (pulsarConnectorConfig.getAuthorizationEnabled() && StringUtils.isEmpty(
                pulsarConnectorConfig.getBrokerBinaryServiceUrl())) {
            throw new IllegalArgumentException(
                    "pulsar.broker-binary-service-url must be present when the pulsar.authorization-enable is true.");
        }
    }

    /**
     * Check if the session has read access to the topic.
     * It will try to subscribe to that topic using the Pulsar Reader to check the consumption privilege.
     * The same topic will only be checked once during the same session.
     */
    public void checkTopicAuth(ConnectorSession session, String topic) {
        Set<String> authorizedTopics =
                authorizedQueryTopicsMap.computeIfAbsent(session.getQueryId(), query -> new HashSet<>());
        if (authorizedTopics.contains(topic)) {
            if (log.isDebugEnabled()) {
                log.debug("The topic %s is already authorized.", topic);
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Checking the authorization for the topic: %s", topic);
        }
        Map<String, String> extraCredentials = session.getIdentity().getExtraCredentials();
        if (extraCredentials.isEmpty()) { // the extraCredentials won't be null
            throw new PrestoException(QUERY_REJECTED,
                    String.format(
                            "Failed to check the authorization for topic %s: The credential information is empty.",
                            topic));
        }
        String authMethod = extraCredentials.get(CREDENTIALS_AUTH_PLUGIN);
        String authParams = extraCredentials.get(CREDENTIALS_AUTH_PARAMS);
        if (StringUtils.isEmpty(authMethod) || StringUtils.isEmpty(authParams)) {
            throw new PrestoException(QUERY_REJECTED,
                    String.format(
                            "Failed to check the authorization for topic %s: Required credential parameters are "
                                    + "missing. Please specify the auth-method and auth-params in the extra "
                                    + "credentials.",
                            topic));
        }
        try {
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarConnectorConfig.getBrokerBinaryServiceUrl())
                    .authentication(authMethod, authParams)
                    .build();
            client.newConsumer().topic(topic)
                    .subscriptionName("pulsar-sql-auth" + session.getQueryId())
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionMode(SubscriptionMode.NonDurable)
                    .startPaused(true)
                    .subscribe()
                    .close();
            authorizedQueryTopicsMap.computeIfPresent(session.getQueryId(), (query, topics) -> {
                topics.add(topic);
                return topics;
            });
            if (log.isDebugEnabled()) {
                log.debug("Check the authorization for the topic %s successfully.", topic);
            }
        } catch (PulsarClientException.AuthenticationException | PulsarClientException.AuthorizationException e) {
            throw new PrestoException(PERMISSION_DENIED,
                    String.format("Failed to access topic %s: %s", topic, e.getLocalizedMessage()));
        } catch (IOException e) {
            throw new PrestoException(QUERY_REJECTED,
                    String.format("Failed to check authorization for topic %s: %s", topic, e.getLocalizedMessage()));
        }
    }

    /**
     * When the session is closed, this method needs to be called to clear the session's auth verification status.
     */
    public void cleanSession(ConnectorSession session) {
        authorizedQueryTopicsMap.remove(session.getQueryId());
    }
}
