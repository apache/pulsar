<!--
RULES
* Never place a link to an external site like Google Doc. The proposal should be in this issue entirely.
* Use a spelling and grammar checker tools if available for you (there are plenty of free ones).

PROPOSAL HEALTH CHECK
I can read the design document and understand the problem statement and what you plan to change *without* resorting to a couple of hours of code reading just to start having a high level understanding of the change.

IMAGES
If you need diagrams, avoid attaching large files. You can use [MermaidJS]([url](https://mermaid.js.org/)) as a simple language to describe many types of diagrams.

THIS COMMENTS
Please remove them when done.
-->

# PIP-427: Align pulsar-admin Default for Mark-Delete Rate with Broker Configuration

# Motivation

The `pulsar-admin namespaces set-persistence` command allows for the configuration of several persistence-related policies on a namespace. One of these policies is the `mark-delete rate`, controlled by the `--ml-mark-delete-max-rate` (or -r) flag.

The command-line help and its current implementation show that the default value for this flag is 0.0:

```
-r, --ml-mark-delete-max-rate=<managedLedgerMaxMarkDeleteRate>
Throttling rate of mark-delete operation (0 means no
throttle)
Default: 0.0
```

Within Pulsar, a managedLedgerMaxMarkDeleteRate of 0 signifies that the rate limit is disabled. This can lead to a "storm" of cursor state writes to BookKeeper, especially with high-frequency acknowledgement patterns, causing significant I/O load, network traffic, and potential cluster instability.

The problem is that if an administrator uses set-persistence to modify another policy (e.g., backlog quotas or ensemble size) and omits the --ml-mark-delete-max-rate flag, the command implicitly applies the default value of 0, thereby disabling any previously configured rate limit. This is a dangerous and counter-intuitive side effect.

A user intending to change one policy can inadvertently disable a critical throttling mechanism, leading to unexpected performance degradation. The broker itself has a safe default (managedLedgerMaxMarkDeleteRate = 1.0), and the admin tool should align with this principle.

# Goals

The goal of this PIP is to change the behavior of the pulsar-admin namespaces set-persistence command.

When set-persistence is invoked, if the `--ml-mark-delete-max-rate` flag is not provided by the user, the command should not reset the mark-delete rate to `0`. Instead, the broker should apply the default rate limit configured at the broker level (via the `managedLedgerMaxMarkDeleteRate` parameter in `broker.conf`).

This change ensures that the command has a safe, predictable default. Users who explicitly wish to disable the rate limit must do so intentionally by providing `--ml-mark-delete-max-rate 0`.

# Proposed Changes

The implementation will require modifications to how the set-persistence command and its corresponding REST endpoint handle the PersistencePolicies object.

1. pulsar-admin CLI Tool:

- The command definition for set-persistence (in CmdNamespaces.java) will be changed. The `--ml-mark-delete-max-rate` parameter's default will be modified from 0.0 to a value indicating it is "unset" (e.g., null or a negative value like -1).

2. Broker Admin REST API:

- The REST endpoint for setting persistence policies (in the Namespaces resource class) will be updated. When it receives the PersistencePolicies payload, it will check if managedLedgerMaxMarkDeleteRate has the "unset" value.
  - The logic will be as follows:
    - If the payload contains an explicit value for managedLedgerMaxMarkDeleteRate (including 0.0), the broker will use that value. This preserves the existing ability to set any specific rate.
    - If the rate is "unset" in the payload, the broker will fetch the default value from its own service configuration (ServiceConfiguration.getManagedLedgerMaxMarkDeleteRate()) and apply that to the namespace's persistence policy.
This makes the broker the source of truth for the default value and prevents accidental resets of the policy.

# Backward & Forward Compatibility

This change introduces a behavioral modification that is considered a bug fix for unsafe default behavior.

- Backward Compatibility: Scripts that used pulsar-admin namespaces set-persistence to update other policies (e.g., --backlog-quota-limit-size) and implicitly relied on it to reset the mark-delete rate to 0 will now see the broker's default rate limit (e.g., 1.0) applied instead. This is the desired outcome. To retain the old behavior of disabling the limit, these scripts must be updated to be explicit: pulsar-admin ... set-persistence --ml-mark-delete-max-rate 0. This change makes a potentially harmful action explicit and intentional.
- Forward Compatibility: No issues are expected. Clusters running the new code will correctly interpret API calls from older clients, as the existing logic for explicitly set values remains unchanged.

And the change will not affect any existing namespaces.

# Alternatives

- Hardcode the CLI default to 1.0: This was rejected because it does not respect a custom default that an administrator may have configured in broker.conf. The broker must be the single source of truth for its default configuration.
- Only update the documentation: This was rejected because it leaves a non-intuitive and potentially harmful default behavior in place for a powerful, multi-purpose command. The principle of least surprise dictates that the tool's behavior should be made safer.

# Links

<!--
Updated afterwards
-->
* Mailing List discussion thread: https://lists.apache.org/thread/j9vx6zkkgnz08sfgp14swylb8wv6djzs
* Mailing List voting thread:
