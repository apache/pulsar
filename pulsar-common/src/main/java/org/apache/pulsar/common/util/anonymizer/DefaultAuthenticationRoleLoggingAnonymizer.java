/*
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
package org.apache.pulsar.common.util.anonymizer;

import static org.apache.pulsar.common.util.anonymizer.DefaultRoleAnonymizerType.NONE;

/**
 * This class provides a utility to anonymize authentication roles before logging,
 * based on a configured anonymization strategy. The anonymization strategy is
 * determined by the provided value of the {@link DefaultRoleAnonymizerType} enum.
 *
 * The primary purpose of this class is to enable flexible anonymization of sensitive
 * information (such as user roles) in logs, ensuring compliance with security and
 * privacy requirements while allowing customization of the anonymization behavior.
 *
 * Usage:
 * - The class constructor accepts a string that represents the desired anonymization
 *   strategy (e.g., "NONE", "REDACTED", "SHA256", "MD5"), and it initializes the
 *   anonymizer type accordingly.
 * - The {@code anonymize} method applies the selected anonymization strategy to
 *   the provided role and returns the anonymized value.
 *
 * Example:
 * <pre>
 * DefaultAuthenticationRoleLoggingAnonymizer roleAnonymizer =
 *     new DefaultAuthenticationRoleLoggingAnonymizer("SHA256");
 * String anonymizedRole = roleAnonymizer.anonymize("admin");
 * </pre>
 *
 * Anonymization strategies:
 * - NONE: No anonymization (returns the role as-is).
 * - REDACTED: Replaces the role with "[REDACTED]".
 * - hash:SHA256: Hashes the role using the SHA-256 algorithm and prefixes it with "SHA-256:".
 * - hash:MD5: Hashes the role using the MD5 algorithm and prefixes it with "MD5:"
 */
public final class DefaultAuthenticationRoleLoggingAnonymizer {
   private static DefaultRoleAnonymizerType anonymizerType = NONE;

   public DefaultAuthenticationRoleLoggingAnonymizer(String authenticationRoleLoggingAnonymizer) {
      if (authenticationRoleLoggingAnonymizer.startsWith("hash:")) {
         anonymizerType = DefaultRoleAnonymizerType.valueOf(authenticationRoleLoggingAnonymizer
                 .substring("hash:".length()));
      } else {
         anonymizerType = DefaultRoleAnonymizerType.valueOf(authenticationRoleLoggingAnonymizer);
      }
   }

   public String anonymize(String role) {
      return anonymizerType.anonymize(role);
   }
}
