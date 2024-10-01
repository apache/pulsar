package org.apache.pulsar.common.util;

public final class RedactedRole {
   private static final String REDACTED = "[REDACTED]";

   public static String anonymize(String role, Boolean preventLogging) {
      return preventLogging ? REDACTED : role;
   }
}
