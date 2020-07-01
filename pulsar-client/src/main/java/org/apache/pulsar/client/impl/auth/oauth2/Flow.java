/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.client.impl.auth.oauth2;

import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult;
import java.io.Serializable;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * An OAuth 2.0 authorization flow.
 */
interface Flow extends Serializable {

    /**
     * Initializes the authorization flow.
     * @throws PulsarClientException if the flow could not be initialized.
     */
    void initialize() throws PulsarClientException;

    /**
     * Acquires an access token from the OAuth 2.0 authorization server.
     * @return a token result including an access token and optionally a refresh token.
     * @throws PulsarClientException if authentication failed.
     */
    TokenResult authenticate() throws PulsarClientException;

    /**
     * Closes the authorization flow.
     */
    void close();
}
