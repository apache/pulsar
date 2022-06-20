---
id: helm-upgrade
title: Upgrade a Pulsar Helm release
sidebar_label: "Upgrade"
original_id: helm-upgrade
---

Before upgrading your Pulsar installation, you need to check the changelog corresponding to the specific release you want to upgrade
to and look for any release notes that might pertain to the new Pulsar chart version.

We also recommend that you need to provide all values using `helm upgrade --set key=value` syntax or `-f values.yml` instead of using `--reuse-values` because some of the current values might be deprecated.

> **NOTE**:
>
> You can retrieve your previous `--set` arguments cleanly, with `helm get values <release-name>`. If you direct this into a file (`helm get values <release-name> > pulsar.yml`), you can safely
pass this file via `-f`. Thus `helm upgrade <release-name> charts/pulsar -f pulsar.yaml`. This safely replaces the behavior of `--reuse-values`.

## Steps

The following are the steps to upgrade Apache Pulsar to a newer version:

1. Check the change log for the specific version you would like to upgrade to
2. Go through [deployment documentation](helm-deploy) step by step
3. Extract your previous `--set` arguments with

   ```bash
   
   helm get values <release-name> > pulsar.yaml
   
   ```

4. Decide on all the values you need to set
5. Perform the upgrade, with all `--set` arguments extracted in step 4

   ```bash
   
   helm upgrade <release-name> charts/pulsar \
       --version <new version> \
       -f pulsar.yaml \
       --set ...
   
   ```

