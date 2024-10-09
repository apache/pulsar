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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public enum DefaultRoleAnonymizerType {
   NONE {
      @Override
      public String anonymize(String role) {
         return role;
      }
   },
   REDACTED {
      @Override
      public String anonymize(String role) {
         return REDACTED_VALUE;
      }
   },
   SHA256 {
      private static final String PREFIX = "SHA-256:";
      private final MessageDigest digest;

      {
         // Initializing the MessageDigest once for SHA-256
         try {
            digest = MessageDigest.getInstance("SHA-256");
         } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
         }
      }

      @Override
      public String anonymize(String role) {
         byte[] hash = digest.digest(role.getBytes());
         return PREFIX + Base64.getEncoder().encodeToString(hash);
      }
   },
   MD5 {
      private static final String PREFIX = "MD5:";
      private final MessageDigest digest;

      {
         // Initializing the MessageDigest once for MD5
         try {
            digest = MessageDigest.getInstance("MD5");
         } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
         }
      }

      @Override
      public String anonymize(String role) {
         byte[] hash = digest.digest(role.getBytes());
         return PREFIX + Base64.getEncoder().encodeToString(hash);
      }
   };

   private static final String REDACTED_VALUE = "[REDACTED]";
   public abstract String anonymize(String role);
}