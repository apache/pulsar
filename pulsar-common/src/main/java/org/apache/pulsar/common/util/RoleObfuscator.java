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
package org.apache.pulsar.common.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * When the Pulsar component is configured with `authenticationRoleAnonymizedInLogging = true`,
 * it hashes the role before logging, preventing the role from being exposed in the logs.
 * Alternatively, if the component is configured with `authenticationRoleRedactedInLogging = true`,
 * it replaces the role with [REDACTED] in the logs.
 */
public final class RoleObfuscator {
   private static final String REDACTED = "[REDACTED]";

   private static final MessageDigest digest;

   static {
      try {
         digest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
         throw new RuntimeException("SHA-256 algorithm not found", e);
      }
   }

   private final boolean authenticationRoleAnonymizedInLogging;
   private final boolean authenticationRoleRedactedInLogging;

   public RoleObfuscator(boolean authenticationRoleAnonymizedInLogging,
                         boolean authenticationRoleRedactedInLogging) {
      this.authenticationRoleAnonymizedInLogging = authenticationRoleAnonymizedInLogging;
      this.authenticationRoleRedactedInLogging = authenticationRoleRedactedInLogging;

   }

   public String obfuscateRole(String role) {
      // Check if the role should be redacted
      if (this.authenticationRoleRedactedInLogging) {
         return REDACTED;
      }

      // Check if the role should be anonymized (hashed)
      if (this.authenticationRoleAnonymizedInLogging) {
         byte[] hash = digest.digest(role.getBytes());
         return Base64.getEncoder().encodeToString(hash);
      }

      // If neither anonymization nor redaction is enabled, return the original role
      return role;
   }
}
