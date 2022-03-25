<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## GitHub Workflows

This directory contains all Pulsar CI checks.

### Required Workflows

When adding new CI workflows, please update the [.asf.yaml](../../.asf.yaml) if the workflow is required to pass before
a PR can be merged. Instructions on how to update the file are below.

This project uses the [.asf.yaml](../../.asf.yaml) to configure which workflows are required to pass before a PR can
be merged. In the `.asf.yaml`, the required contexts are defined in the `github.protected_branches.*.required_status_checks.contexts.[]`
where * is any key in the `protected_branches` map.

You can view the currently required status checks by running the following command:

```shell
curl -s -H 'Accept: application/vnd.github.v3+json' https://api.github.com/repos/apache/pulsar/branches/master | \
jq .protection
```

These contexts get their names in one of two ways depending on how the workflow file is written in this directory. The
following command will print out the names of each file and the associated with the check. If the `name` field is `null`,
the context will be named by the `id`.

```shell
for f in .github/workflows/*.yaml; \
do FILE=$f yq eval -o j '.jobs | to_entries | {"file": env(FILE),"id":.[].key, "name":.[].value.name}' $f; \
done
```

Duplicate names are allowed, and all checks with the same name will be treated the same (required or not required).