# PIP-385: Role Anonymizer for Pulsar Logging

# Background knowledge

In distributed systems, logging is a critical aspect of monitoring, troubleshooting, and auditing. However, it’s equally important to protect sensitive information, such as authentication roles, in logs. In Pulsar, token-based authentication is widely used, and the role associated with the token can appear in logs.

To enhance privacy and comply with security regulations, it’s necessary to anonymize authentication roles in logs. Anonymization ensures that sensitive details are hidden while still allowing meaningful analysis of logs for operational purposes.

This PIP introduces the **Role Anonymizer** feature in Pulsar, providing different levels of anonymization for roles before they are logged in the broker and proxy components. The anonymizer supports the following modes:
- **NONE**: No anonymization, roles are logged as-is.
- **REDACTED**: Replaces the role with `[REDACTED]`.
- **SHA256**: Hashes the role using the SHA-256 algorithm.
- **MD5**: Hashes the role using the MD5 algorithm.

This feature allows operators to configure the level of anonymization based on their compliance needs without changing the core logging infrastructure.

# Motivation

The current Pulsar logging mechanism logs authentication roles in plain text. This can expose sensitive information, especially in environments where logs are centrally aggregated or monitored by third parties. It is essential to anonymize these roles to prevent potential misuse or unauthorized access to role information from logs.

The main problem this proposal solves is the risk of exposing sensitive information (such as user roles) through logs. Anonymizing roles in logs reduces this risk while maintaining useful logs for debugging and operational monitoring.

# Goals

## In Scope

- Introduce a configurable anonymizer for roles in logs for both broker and proxy components.
- Support multiple anonymization strategies, including no anonymization, redaction, and hashing using SHA-256 and MD5.
- Ensure that the anonymization strategy is easily configurable through the existing configuration files.

## Out of Scope

- This proposal does not cover anonymization of other sensitive fields beyond roles in logs.
- No changes will be made to non-logging aspects of the authentication process.

# High Level Design

This feature adds a configurable anonymization layer to Pulsar’s logging mechanism. The anonymization logic will be applied to the role field during the logging of authentication information on both brokers and proxies.

The anonymization strategy will be defined through a configuration parameter (`authenticationRoleLoggingAnonymizer`) and can be set to one of the following values:
- `NONE`: Logs the role without modification.
- `REDACTED`: Logs `[REDACTED]` instead of the actual role.
- `hash:SHA256`: Logs a SHA-256 hash of the role.
- `hash:MD5`: Logs an MD5 hash of the role.

The default strategy is `NONE`, meaning no anonymization will be applied unless explicitly configured.

# Detailed Design

## Design & Implementation Details

The `DefaultAuthenticationRoleLoggingAnonymizer` class will be introduced to handle the anonymization of roles in logs. This class will accept a configuration parameter to select the anonymization strategy, and apply the corresponding transformation to the role before it is logged.

### Core Components:
1. **`DefaultRoleAnonymizerType` Enum**: Defines the available anonymization strategies (`NONE`, `REDACTED`, `SHA256`, `MD5`).
2. **`DefaultAuthenticationRoleLoggingAnonymizer` Class**: Handles the anonymization process by selecting and applying the chosen strategy based on the configuration.
3. **Broker and Proxy Configuration**: New configuration options will be added to both the broker and proxy configuration files, allowing administrators to specify the desired anonymization strategy.

### Code Example:
```java
// Anonymizer logic
public final class DefaultAuthenticationRoleLoggingAnonymizer {
   private static DefaultRoleAnonymizerType anonymizerType = NONE;

   public DefaultAuthenticationRoleLoggingAnonymizer(String authenticationRoleLoggingAnonymizer) {
      if (authenticationRoleLoggingAnonymizer.startsWith("hash:")) {
         anonymizerType = DefaultRoleAnonymizerType.valueOf(authenticationRoleLoggingAnonymizer
                 .substring("hash:".length()).toUpperCase());
      } else {
         anonymizerType = DefaultRoleAnonymizerType.valueOf(authenticationRoleLoggingAnonymizer);
      }
   }

   public String anonymize(String role) {
      return anonymizerType.anonymize(role);
   }
}
```

## Public-facing Changes

The following public-facing components will be affected:

### Public API

This PIP does not introduce changes to the public API. The anonymization functionality only affects the internal logging of the Pulsar broker and proxy components.

### Configuration

New configuration options will be added to both the broker and proxy configuration files to control the role anonymization strategy. These options are as follows:

**Broker Configuration:**
```yaml
authenticationRoleLoggingAnonymizer: "NONE" 
# Options: NONE, REDACTED, hash:SHA256, hash:MD5
```

**Proxy Configuration**
```yaml
authenticationRoleLoggingAnonymizer: "NONE"
# Options: NONE, REDACTED, hash:SHA256, hash:MD5
```

# Monitoring
Administrators can monitor anonymized logs to ensure that roles are being anonymized according to the configuration. Logs should be checked to verify the correct anonymization strategy is applied.

# Security Considerations
This feature strengthens security by preventing sensitive role information from being exposed in logs. However, care should be taken to select an appropriate anonymization strategy that balances security and operational needs. For example, hashing strategies like SHA-256 provide stronger anonymization compared to MD5.

# Backward & Forward Compatibility

## Upgrade

No special upgrade instructions are needed. The new configuration parameter will default to NONE, ensuring backward compatibility.

## Downgrade / Rollback
No special rollback instructions are required. The anonymizer will only take effect when the configuration parameter is set, so downgrading will simply result in roles being logged in plain text.

## Alternatives
One alternative considered was redacting roles entirely without offering hashing options. This was rejected because it would reduce the usefulness of logs for operational monitoring, particularly in environments where roles need to be traced without revealing their actual values.
