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

# Release Process

This page contains instructions for Pulsar committers on how to perform a release.

> **NOTE**: The term `major/minor releases` used throughout this document is defined as follows:
>
> * Major releases refer to feature releases, such as 2.10.0, 2.11.0, and so on.
> 
> * Minor releases refer to bug-fix releases, such as 2.10.1, 2.10.2, and so on.


## Preparation

Open a discussion in dev@apache.org to notify others that you volunteer to be the release manager of a specific release. If there are no disagreements, you can start the release process.

For major releases, you should create a new branch named `branch-2.X.0` once all PRs with the 2.X.0 milestone are merged. If some PRs with the 2.X.0 milestone are still working in progress and might take much time to complete, you can move them to the next milestone if they are not important. In this case, you'd better notify the author in the PR.

For minor releases, if there are no disagreements, you should cherry-pick all merged PRs with the `release/X.Y.Z` labels into `branch-X.Y`. After these PRs are cherry-picked, you should add the `cherry-picked/branch-X.Y` labels.

Sometimes some PRs cannot be cherry-picked cleanly, you might need to create a separate PR and move the `release/X.Y.Z` label from the original PR to it. In this case, you can ask the author to help create the new PR.

For PRs that are still open, you can choose to delay them to the next release or ping others to review so that they can be merged.

To verify the release branch is not broken, you can synchronize the branch in your private repo and open a PR to trigger the CI. Example: https://github.com/BewareMyPower/pulsar/pull/3

> You can use the following command to catch basic compilation or checkstyle errors in your local env before cherry-picking.
>
> ```bash
> mvn -Pcore-modules,-main -T 1C clean install -DskipTests -Dspotbugs.skip=true
> ```

## Requirements

If you haven't already done it, [create and publish the GPG key](https://github.com/apache/pulsar/blob/master/wiki/release/create-gpg-keys.md) to sign the release artifacts.

Before you start the next release steps, make sure you have installed the **JDK8** and maven **3.6.1** for Pulsar 2.6 and Pulsar 2.7, and **JDK11** and Maven **3.6.1** for Pulsar 2.8 onwards. And **clean up the bookkeeper's local compiled** to make sure the bookkeeper dependency is fetched from the Maven repo, details to see https://lists.apache.org/thread/gsbh95b2d9xtcg5fmtxpm9k9q6w68gd2

## Create the release branch

We are going to create a branch from `master` to `branch-v2.X`
where the tag will be generated and where new fixes will be
applied as part of the maintenance for the release.

The branch needs only to be created when creating major releases,
and not for minor releases like `2.3.1`. For minor releases, go to the next step.

For example, when you create the `v2.3.0` release, the branch `branch-2.3` will be created; but for `v2.3.1`, we
keep using the old `branch-2.3`.

In these instructions, I'm referring to a fictitious release `2.X.0`. Change the release version in the examples accordingly with the real version.

It is recommended to create a fresh clone of the repository to avoid any local files interfering in the process:

```shell
git clone git@github.com:apache/pulsar.git
cd pulsar
git checkout -b branch-2.X origin/master
```

Alternatively, you can use a git workspace to create a new, clean directory on your machine without needing to re-download the project.

```shell
git worktree add ../pulsar.branch-2.X branch-2.X
```

If you created a new branch, update the `CI - OWASP Dependency Check` workflow so that it will run on the new branch. At the time of writing, here is the file that should be updated: https://github.com/apache/pulsar/blob/master/.github/workflows/ci-owasp-dependency-check.yaml.

Note also that you should stop the GitHub action for Pulsar versions that are EOL.

Also, if you created a new branch, please update the `Security Policy and Supported Versions` page on the website. This page has a table for support timelines based on when minor releases take place.

## Update project version and tag

During the release process, you are going to initially create
"candidate" tags, that after verification and approval will
get promoted to the "real" final tag.

In this process, the maven version of the project will always
be the final one.

```shell
# Bump to the release version
./src/set-project-version.sh 2.X.0

# Some version may not update the right parent version of `protobuf-shaded/pom.xml`, please double check it.

# Commit
git commit -m 'Release 2.X.0' -a

# Create a "candidate" tag
# If you don't sign your commits already, use the following
export GPG_TTY=$(tty)
git tag -u $USER@apache.org v2.X.0-candidate-1 -m 'Release v2.X.0-candidate-1'
# If you already sign your commits using your apache.org email, use the following
git tag -s v2.X.0-candidate-1 -m 'Release v2.X.0-candidate-1'

# Verify that you signed your tag before pushing it:
git tag -v v2.X.0-candidate-1

# Push both the branch and the tag to Github repo
git push origin branch-2.X
git push origin v2.X.0-candidate-1
```

For minor releases, the tag is like `2.3.1`.

## Build and inspect the artifacts

```shell
mvn clean install -DskipTests
```

After the build, there will be 4 generated artifacts:

* `distribution/server/target/apache-pulsar-2.X.0-bin.tar.gz`
* `target/apache-pulsar-2.X.0-src.tar.gz`
* `distribution/offloaders/target/apache-pulsar-offloaders-2.X.0-bin.tar.gz`
* directory `distribution/io/target/apache-pulsar-io-connectors-2.x.0-bin` contains all io connect nars

Inspect the artifacts:
* Check that the `LICENSE` and `NOTICE` files cover all included jars for the -bin package)
    - Use script to cross-validate `LICENSE` file with included jars:
       ```
       src/check-binary-license distribution/server/target/apache-pulsar-2.x.0-bin.tar.gz
       ```
* Unpack src package: `target/apache-pulsar-2.X.0-src.tar.gz`
    - Run Apache RAT to verify the license headers in the `src` package:
       ```shell
       cd apache-pulsar-2.X.0
       mvn apache-rat:check
       ```
* Unpack bin package: `distribution/server/target/apache-pulsar-2.X.0-bin.tar.gz`, Check that the standalone Pulsar service starts correctly:
 ```shell
 cd apache-pulsar-2.X.0
 cp -r ../../../io/target/apache-pulsar-io-connectors-2.X.0-bin connectors
 bin/pulsar standalone
 ```

* Use instructions in [Release-Candidate-Validation](https://github.com/apache/pulsar/blob/master/wiki/release/release-candidate-validation.md) to do some sanity checks on the produced binary distributions.

### Build RPM and DEB packages

```shell
pulsar-client-cpp/pkg/rpm/docker-build-rpm.sh

pulsar-client-cpp/pkg/deb/docker-build-deb.sh
```

> For 2.11.0 or higher, you can set the environment variable `BUILD_IMAGE` to build the base image locally instead of pulling from the DockerHub.
> Since only a few members have the permission to push the image to DockerHub, the image might not be the latest, if you failed to build the RPM and DEB packages, you can run `export BUILD_IMAGE=1` before running these commands.

This will leave the RPM/YUM and DEB repo files in `pulsar-client-cpp/pkg/rpm/RPMS/x86_64` and
`pulsar-client-cpp/pkg/deb/BUILD/DEB` directory.

> **NOTE**: If you get error `c++: internal compiler error: Killed (program cc1plus)` when run `pulsar-client-cpp/pkg/deb/docker-build-deb.sh`. You may need to expand your docker memory greater than 2GB.

## Sign and stage the artifacts

The `src` and `bin` artifacts need to be signed and uploaded to the dist SVN
repository for staging.

Before running the script, make sure that the `user@apache.org` code signing key is the default gpg signing key.
One way to ensure this is to create/edit file `~/.gnupg/gpg.conf` and add a line
```
default-key <key fingerprint>
```
where `<key fingerprint>` should be replaced with the private key fingerprint for the `user@apache.org` key. The key fingerprint can be found in `gpg -K` output.

```shell
svn co https://dist.apache.org/repos/dist/dev/pulsar pulsar-dist-dev
cd pulsar-dist-dev

# '-candidate-1' needs to be incremented in case of multiple iteration in getting
#    to the final release)
svn mkdir pulsar-2.X.0-candidate-1

cd pulsar-2.X.0-candidate-1
$PULSAR_PATH/src/stage-release.sh .

svn add *
svn ci -m 'Staging artifacts and signature for Pulsar release 2.X.0'
```

## Stage artifacts in maven

Upload the artifacts to ASF Nexus:

```shell
# remove CPP client binaries (they would file the license/RAT check in "deploy")
cd pulsar-client-cpp
git clean -xfd
cd ..

export APACHE_USER=$USER
export APACHE_PASSWORD=$MY_PASSWORD
export GPG_TTY=$(tty)
# src/settings.xml from master branch to /tmp/mvn-apache-settings.xml since it's missing in some branches
curl -s -o /tmp/mvn-apache-settings.xml https://raw.githubusercontent.com/apache/pulsar/master/src/settings.xml
# publish artifacts
mvn deploy -DskipTests -Papache-release --settings /tmp/mvn-apache-settings.xml
# publish org.apache.pulsar.tests:integration and it's parent pom org.apache.pulsar.tests:tests-parent
mvn deploy -DskipTests -Papache-release --settings /tmp/mvn-apache-settings.xml -f tests/pom.xml -pl org.apache.pulsar.tests:tests-parent,org.apache.pulsar.tests:integration
```

> **NOTE**: The `GPG_TTY` environment variable must be set for all the following steps. Otherwise, some operations might fail by `gpg failed to sign the data`.

This will ask for the GPG key passphrase and then upload it to the staging repository.

> If you have deployed before, re-deploying might fail on pulsar-presto-connector-original.
>
> See https://github.com/apache/pulsar/issues/17047.
>
> You can run `mvn clean deploy` instead of `mvn deploy` as a workaround.

Log in to the ASF Nexus repository at https://repository.apache.org

Click on "Staging Repositories" on the left sidebar and then select the current
Pulsar staging repo. This should be called something like `orgapachepulsar-XYZ`.

Use the "Close" button to close the repository. This operation will take few
minutes. Once complete click "Refresh" and now a link to the staging repository
should be available, something like
https://repository.apache.org/content/repositories/orgapachepulsar-XYZ

## Publish release candidate docker images

Run the following commands:

```shell
cd $PULSAR_HOME/docker
./build.sh
DOCKER_USER=<your-username> DOCKER_PASSWORD=<your-password> DOCKER_ORG=<your-username> ./publish.sh
```

After that, the following images will be built and pushed to your own DockerHub account.
- pulsar
- pulsar-all
- pulsar-grafana
- pulsar-standalone


## Run the vote

Send an email to the Pulsar Dev mailing list:

```
To: dev@pulsar.apache.org
Subject: [VOTE] Pulsar Release 2.X.0 Candidate 1

This is the first release candidate for Apache Pulsar, version 2.X.0.

It fixes the following issues:
https://github.com/apache/pulsar/milestone/8?closed=1

*** Please download, test and vote on this release. This vote will stay open
for at least 72 hours ***

Note that we are voting upon the source (tag), binaries are provided for
convenience.

Source and binary files:
https://dist.apache.org/repos/dist/dev/pulsar/pulsar-2.X.0-candidate-1/

SHA-512 checksums:
028313cbbb24c5647e85a6df58a48d3c560aacc9  apache-pulsar-2.X.0-SNAPSHOT-bin.tar.gz
f7cc55137281d5257e3c8127e1bc7016408834b1  apache-pulsar-2.x.0-SNAPSHOT-src.tar.gz

Maven staging repo:
https://repository.apache.org/content/repositories/orgapachepulsar-169/

The tag to be voted upon:
v2.X.0-candidate-1 (21f4a4cffefaa9391b79d79a7849da9c539af834)
https://github.com/apache/pulsar/releases/tag/v2.X.0-candidate-1

Pulsar's KEYS file containing PGP keys you use to sign the release:
https://dist.apache.org/repos/dist/dev/pulsar/KEYS

Docker images:

<link of the pulsar images>

<link of the pulsar-all image>

Please download the source package, and follow the README to build
and run the Pulsar standalone service.
```

The vote should be open for at least 72 hours (3 days). Votes from Pulsar PMC members
will be considered binding, while anyone else is encouraged to verify the release and
vote as well.

If the release is approved here, you can then proceed to the next step. Otherwise, you should repeat the previous steps and prepare another candidate release to vote.

## Move master branch to next version

> **NOTE**: This step is for major releases only.

You need to move the master version to the next iteration `Y` (`X + 1`).

```shell
git checkout master
./src/set-project-version.sh 2.Y.0-SNAPSHOT

git commit -m 'Bumped version to 2.Y.0-SNAPSHOT' -a
```

Since this needs to be merged into `master`, you need to follow the regular process
and create a Pull Request on GitHub.

## Promote the release

Create the final git tag:

```shell
git tag -u $USER@apache.org v2.X.0 -m 'Release v2.X.0'
git push origin v2.X.0
```

Promote the artifacts on the release location(repo https://dist.apache.org/repos/dist/release limited to PMC, You may need a PMC member's help if you are not one):
```shell
svn move -m "Release Apache Pulsar 2.X.Y" https://dist.apache.org/repos/dist/dev/pulsar/pulsar-2.X.0-candidate-1 \
         https://dist.apache.org/repos/dist/release/pulsar/pulsar-2.X.0
```

Promote the Maven staging repository for release. Login to `https://repository.apache.org` and
select the staging repository associated with the RC candidate that was approved. The naming
will be like `orgapachepulsar-XYZ`. Select the repository and click on "Release". Artifacts
will now be made available on Maven central.

## Publish Docker Images

Copy the approved candidate docker images from your personal account to apachepulsar org.

```bash
PULSAR_VERSION=2.x.x
OTHER_DOCKER_USER=otheruser
for image in pulsar pulsar-all pulsar-grafana pulsar-standalone; do
    docker pull "${OTHER_DOCKER_USER}/$image:${PULSAR_VERSION}" && {
      docker tag "${OTHER_DOCKER_USER}/$image:${PULSAR_VERSION}" "apachepulsar/$image:${PULSAR_VERSION}"
      echo "Pushing apachepulsar/$image:${PULSAR_VERSION}"
      docker push "apachepulsar/$image:${PULSAR_VERSION}"
    }
done
```

If you don't have the permission, you can ask someone with access to apachepulsar org to do that.

## Release Helm Chart

**This step can be skipped if the major version number is not the latest.**

1. Bump the image version in the Helm Chart: [charts/pulsar/values.yaml](https://github.com/apache/pulsar-helm-chart/blob/master/charts/pulsar/values.yaml)

2. Bump the chart version and `appVersion` in the Helm Chart to the released version: [charts/pulsar/Chart.yaml](https://github.com/apache/pulsar-helm-chart/blob/master/charts/pulsar/Chart.yaml)

3. Send a pull request for reviews and get it merged.

4. Once it is merged, the chart will be automatically released to Github releases at https://github.com/apache/pulsar-helm-chart and updated to https://pulsar.apache.org/charts.

## Publish Python Clients

> **NOTES**:
>
> 1. You need to create an account on PyPI: https://pypi.org/account/register/
>
> 2. Ask anyone that has been a release manager before to add you as a maintainer for pulsar-docker on PyPI
>
> 3. Once you have completed the following steps in this section, you can check if the wheels are uploaded successfully in [Download files](https://pypi.org/project/pulsar-client/#files). Remember to switch to the correct version in [Release history](https://pypi.org/project/pulsar-client/#history)).

### Linux

There is a script that builds and packages the Python client inside Docker images.

> Make sure you run following command at the release tag!!

```shell
pulsar-client-cpp/docker/build-wheels.sh
```

The wheel files will be left under `pulsar-client-cpp/python/wheelhouse`. Make sure all the files have `manylinux` in the filenames. Otherwise, those files will not be able to upload to PyPI.

Run the following command to push the built wheel files.

```shell
cd pulsar-client-cpp/python/wheelhouse
pip install twine
twine upload pulsar_client-*.whl
```

### MacOS

There is a script that builds and packages the Python client inside Docker images.

```shell
pulsar-client-cpp/python/build-mac-wheels.sh
```

The wheel files will be generated at each platform directory under `pulsar-client-cpp/python/pkg/osx/`.
Then you can run `twin upload` to upload those wheel files.

## Update Python Client docs

After publishing the python client docs, run the following script from the apache/pulsar-site `main` branch:

```shell
PULSAR_VERSION=2.X.Y ./site2/tools/api/python/build-docs-in-docker.sh
```

Note that it builds the docs within a docker image, so you'll need to have docker running.

Once the docs are generated, you can add them and submit them in a PR. The expected doc output is `site2/website/static/api/python`.

## Publish Homebrew libpulsar package

**This step can be skipped if the major version number is not the latest.**

Release a new version of libpulsar for Homebrew, You can follow the example [here](https://github.com/Homebrew/homebrew-core/pull/53514).

## Update swagger file

> For major releases, the swagger file update happen under `master` branch.
> while for minor releases, swagger file is created from branch-2.x, and need copy to a new branch based on master.

```shell
git checkout branch-2.X
mvn -am -pl pulsar-broker install -DskipTests -Pswagger
git checkout master
git checkout -b fix/swagger-file
mkdir -p site2/website/static/swagger/2.X.0
cp pulsar-broker/target/docs/*.json site2/website/static/swagger/2.X.0
```
Send out a PR request for review.

## Write release notes

See [Pulsar Release Notes Guide](https://docs.google.com/document/d/1cwNkBefKyV6OPbEXnUrcCdVZi0i2BezqL6vAL7VqVC0/edit#).

## Update the site

> **NOTE**: This step is for major releases only.

For major releases, such as 2.10.0, the website is updated based on the `master` branch.

1. Create a new branch off master.

```shell
git checkout -b doc_release_<release-version>
```

2. Go to the website directory.

```shell
cd site2/website
```

3. Generate a new version of the documentation.

```shell
yarn install
yarn run version <release-version>
```

After you run this command, a new folder `version-<release-version>` is added in the `site2/website/versioned_docs` directory, a new sidebar file `version-<release-version>-sidebars.json` is added in the `site2/website/versioned_sidebars` directory, and the new version is added in the `versions.json` file, shown as follows:

  ```shell
  versioned_docs/version-<release-version>
  versioned_sidebars/version-<release-version>-sidebars.json 
  ```

> **NOTE**: You can move the latest version under the old version in the `versions.json` file. Make sure the Algolia index works before moving 2.X.0 as the current stable.

4. Update the `releases.json` file by adding `<release-version>` to the second of the list (this is to make the search work. After your PR is merged, the Pulsar website is built and tagged for search, you can change it to the first list).

5. Send out a PR request for review.

   After your PR is approved and merged to master, the website is published automatically after the new website is built. The website is built every 6 hours.

6. Check the new website after the website is built.  
   Open https://pulsar.apache.org in your browsers to verify all the changes are alive. If the website build succeeds but the website is not updated, you can try to sync the git repository. Navigate to https://selfserve.apache.org/ and click the "Synchronize Git Repositories" and then select apache/pulsar.

7. Publish the release on GitHub, and copy the same release notes: https://github.com/apache/pulsar/releases.

8. Update the deploy version to the current release version in `deployment/terraform-ansible/deploy-pulsar.yaml`.

9. Generate the doc set and sidebar file for the next major release `2.X.x` based on the `site2/docs` folder. You can follow steps 1, 2, and 3, and submit those files to the `apache/pulsar` repository. This step is a preparation for the `2.X.x` release.

> **NOTE**: Starting from 2.8.0, you don't need to generate an independent doc set or update the Pulsar site for minor releases, such as 2.8.1, 2.8.2, and so on. Instead, the generic doc set 2.8.x is used.

## Announce the release

Once the release artifacts are available in the Apache Mirrors and the website is updated,
we need to announce the release.

Send an email to these lines:

```
To: dev@pulsar.apache.org, users@pulsar.apache.org, announce@apache.org
Subject: [ANNOUNCE] Apache Pulsar 2.X.0 released

The Apache Pulsar team is proud to announce Apache Pulsar version 2.X.0.

Pulsar is a highly scalable, low latency messaging platform running on
commodity hardware. It provides simple pub-sub semantics over topics,
guaranteed at-least-once delivery of messages, automatic cursor management for
subscribers, and cross-datacenter replication.

For Pulsar release details and downloads, visit:

https://pulsar.apache.org/download

Release Notes are at:
https://pulsar.apache.org/release-notes

We would like to thank the contributors that made the release possible.

Regards,

The Pulsar Team
```

Send the email in plain text mode since the announce@apache.org mailing list will reject messages with text/html content.
In Gmail, there's an option to set `Plain text mode` in the `⋮`/ `More options` menu.


## Write a blog post for the release (optional)

It is encouraged to write a blog post to summarize the features introduced in this release,
especially for feature releases.
You can follow the example [here](https://github.com/apache/pulsar/pull/2308)

## Remove old releases

Remove the old releases (if any). You only need the latest release there, and older releases are
available through the Apache archive:

```shell
# Get the list of releases
svn ls https://dist.apache.org/repos/dist/release/pulsar

# Delete each release (except for the last one)
svn rm https://dist.apache.org/repos/dist/release/pulsar/pulsar-2.Y.0
```

## Move release branch to next version

Run the following commands in the release branches.

```shell
./src/set-project-version.sh 2.X.Y-SNAPSHOT

git commit -m 'Bumped version to 2.X.Y-SNAPSHOT' -a
git push origin branch-2.X
```
