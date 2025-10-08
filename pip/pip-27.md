# PIP-27: Add checklist in github pull request template
* Status: Accepted
* Author: [Sijie Guo](https://github.com/sijie)
* Pull Request: N/A
* Mailing List Discussion: N/A
* Release: N/A

## Motivation

With the increase of contributions, more and more features are added pretty quickly. However these features
are not documented. And there is no checklist for contributors to check when contributing a feature. This PIP
is proposing to improve the github pull request template by adding a checklist for contributors to check. It also help improve the review process.

## Github Pull Request Template

```
<--
## Contribution Checklist

  - Name the pull request in the form "[component] Title of the pull request". Skip *component* if you are unsure about which is the best component. E.g. `[docs] Fix typo in produce method`.

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.

  - Make sure that the change passes the CI checks.

  - Each pull request should address only one issue, not mix up code from multiple issues.

  - Each commit in the pull request has a meaningful commit message

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.


**(The sections below can be removed for hotfixes of typos)**
-->

## Motivation

*Explain here the context, and why you're making that change. What is the problem you're trying to solve.*

## Modifications

*Describe the modifications you've done.*

## Verifying this change

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added integration tests for end-to-end deployment with large payloads (10MB)*
  - *Extended integration test for recovery after broker failure*

## Does this pull request potentially affect one of the following parts:

*If `yes` was chosen, please highlight the changes*

  - Dependencies (does it add or upgrade a dependency): (yes / no)
  - The public API: (yes / no)
  - The schema: (yes / no / don't know)
  - The default values of configurations: (yes / no)
  - The wire protocol: (yes / no)
  - The rest endpoints: (yes / no)
  - The admin cli options: (yes / no)
  - Anything that affects deployment: (yes / no / don't know)

## Documentation

  - Does this pull request introduce a new feature? (yes / no)
  - If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)

```
