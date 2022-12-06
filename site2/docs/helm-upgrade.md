---
id: helm-upgrade
title: Upgrade Pulsar Helm release
sidebar_label: "Upgrade"
---


To upgrade Apache Pulsar Helm to a newer version, complete the following steps:

1. Check the change log for the specific version you would like to upgrade to and read the release notes that might pertain to the new Pulsar Helm Chart version.

2. Go through the [deployment guide](helm-deploy.md) step by step.

3. Extract your previous `--set` arguments with the following command and change the values if required.

   ```bash
   helm get values <release-name> > pulsar.yaml
   ```

   :::note

   It's highly recommended to provide all values using the `helm upgrade --set key=value` syntax or the `-f values.yml` instead of using `--reuse-values`, because some of the current values might be deprecated.

   You can retrieve your previous `--set` arguments cleanly, with `helm get values <release-name>`. If you direct this into a file (`helm get values <release-name> > pulsar.yml`), you can safely pass this file through `-f`, namely `helm upgrade <release-name> apache/pulsar -f pulsar.yaml`. This safely replaces the behavior of `--reuse-values`.

   :::

4. Perform the upgrade, with all `--set` arguments extracted in step 3.

   ```bash
   helm upgrade <release-name> apache/pulsar \
       --version <new version> \
       -f pulsar.yaml \
       --set ...
   ```

