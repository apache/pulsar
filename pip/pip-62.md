# PIP-62: Move connectors, adapters and Pulsar Presto to separate repositories

- Status: Proposal
- Author: Sijie Guo
- Pull Request: 
- Mailing List discussion:
- Release: 2.6.0

## Motivation

Currently, Pulsar provides a lot of built-in connectors and adapters in the main repo. These connectors and adapters increase the build time of the entire project significantly. Pulsar SQL is also effectively a presto connector, which provides a convenient way for people to connect Presto with Pulsar and use Presto to process and query events stored in Pulsar.

The skillsets of developing core Pulsar messaging and storage functionalities are a bit different from developing connectors, adapters, and integration for the pulsar. 

This PIP is proposing moving connectors, adapters and Pulsar Presto separate repositories. This allows:

- Reduce the build time of the main project.
- Speed up the development of core components, the connectors, and adapters.
- Allow a fast review process for different components.

## Proposal

Here is a detailed proposal.

### Repositories

I am proposing creating three repositories.

- `pulsar-connectors`: This is the repository for hosting the development of Pulsar connectors.
- `pulsar-adapters`: This is the repository for hosting the development of adapters, which includes Kafka compatible client, pulsar-spark, pulsar-flink, and pulsar-storm integration. 
- `pulsar-sql`: This is the repository for hosting the development of the Pulsar Presto connector.

All these three repositories are built using the Pulsar libraries in the latest master of main Pulsar repo.  

### Release

All these four pulsar repositories (`pulsar`, `pulsar-connectors`, `pulsar-adapters` and `pulsar-sql`) will still be released in one single release as usual. The release script will be updated to be able to release one single release from all four pulsar repositories. 

The main release script will be hosted in `pulsar-test-infra` repo (which was already used for hosting our CI scripts). 

### CI

The three new repositories will run their Github Action based CI tests. The CI tests will use the latest build from the main pulsar repo. The `pulsarbot` can be enhanced to add a common action to install Pulsar client dependencies from main repo before running any actions.
