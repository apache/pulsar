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
package org.apache.pulsar.client.impl.auth.oauth2.protocol;

import java.io.IOException;

/**
 * An interface for exchanging client credentials for an access token.
 */
public interface ClientCredentialsExchanger {
    /**
     * Requests an exchange of client credentials for an access token.
     * @param req the request details.
     * @return an access token.
     * @throws TokenExchangeException if the OAuth server returned a detailed error.
     * @throws IOException if a general IO error occurred.
     */
    TokenResult exchangeClientCredentials(ClientCredentialsExchangeRequest req)
            throws TokenExchangeException, IOException;

    /**
     * Closes the exchanger.
     */
    void close();
}
