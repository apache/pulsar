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

const DEFAULT_READY_TO_TEST_LABEL = 'ready-to-test';
const DEFAULT_TRUNK_BRANCHES = 'master,branch-*,pulsar-*';

// GitHub stacked pull requests: https://docs.github.com/en/pull-requests/how-tos/stacked-pull-requests
// The pull request at position 1 is the bottom of the stack, the only one based on the stack's trunk branch.
const STACK_QUERY = `
  query($owner: String!, $repo: String!, $number: Int!) {
    repository(owner: $owner, name: $repo) {
      pullRequest(number: $number) {
        stackEntry {
          position
        }
        stack {
          number
          size
          baseRefName
          entries(first: 100) {
            nodes {
              position
              pullRequest {
                number
                url
              }
            }
          }
        }
      }
    }
  }`;

function parsePatterns(value) {
  return (value || '').split(/[\s,]+/).filter(pattern => pattern.length > 0);
}

function matchesAnyPattern(branch, patterns) {
  return patterns.some(pattern => {
    const regex = new RegExp(`^${pattern.split('*').map(escapeRegExp).join('.*')}$`);
    return regex.test(branch);
  });
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Resolves the position of the pull request within a GitHub stack.
 * The stack fields aren't available in all GitHub deployments, in that case `available` is false and
 * the caller falls back to inspecting the base branch of the pull request.
 */
async function resolveStack({ github, core, owner, repo, number }) {
  let response;
  try {
    response = await github.graphql(STACK_QUERY, { owner, repo, number });
  } catch (error) {
    core.warning(`Couldn't resolve GitHub stack information for #${number}: ${error.message}`);
    return { available: false };
  }
  const stack = response?.repository?.pullRequest?.stack;
  if (!stack) {
    return { available: true, inStack: false };
  }
  const entries = (stack.entries?.nodes || []).filter(entry => entry);
  const bottomEntry = entries.find(entry => entry.position === 1);
  const position = response.repository.pullRequest.stackEntry?.position;
  return {
    available: true,
    inStack: true,
    // when the position isn't reported, fall back to comparing the base branch with the stack's trunk branch
    isBottom: position != null ? position === 1 : bottomEntry?.pullRequest?.number === number,
    position,
    number: stack.number,
    size: stack.size,
    trunkBranch: stack.baseRefName,
    bottomPullRequest: bottomEntry?.pullRequest
  };
}

function renderSummary({ pullRequestUrl, blockers, label }) {
  const steps = [
    ...blockers.map(blocker => blocker.remedy),
    `Test the change in your own fork in the meantime. The full CI pipeline runs in a fork without `
      + `maintainer approval and GitHub Actions provides separate quota for it. See the `
      + `[Personal CI documentation](https://pulsar.apache.org/contribute/personal-ci/) for enabling `
      + `it: push the branch to your fork and let the CI run against the pull request opened in your `
      + `own fork. As the pull request author, you are responsible for following up on test failures. `
      + `Please report any flaky tests as new issues at https://github.com/apache/pulsar/issues after `
      + `checking that the flaky test isn't already reported.`,
    `An Apache Pulsar committer can add the \`${label}\` label to ${pullRequestUrl} to run the CI in `
      + `apache/pulsar regardless of the checks above.`,
    `This workflow doesn't restart on its own when the pull request is marked as ready for review, `
      + `when the \`${label}\` label is added or when the pull request below this one in a stack is `
      + `merged. Once the checks above are addressed, start a new run by pushing to the branch, by `
      + `adding a "/pulsarbot rerun" comment to the pull request, or by re-running the failed jobs in `
      + `the GitHub Actions UI.`
  ];
  return `
## Pulsar CI didn't run for this pull request

The apache/pulsar CI based on GitHub Actions has constrained resources and quota which are shared by
all contributors, so CI in apache/pulsar is reserved for pull requests that are ready to be tested:
draft pull requests, and the pull requests of a stack above the bottom one, are expected to be tested
with [Personal CI](https://pulsar.apache.org/contribute/personal-ci/) in the contributor's own fork.

### Why this run was stopped

${blockers.map(blocker => `- ${blocker.reason}`).join('\n')}

### How to proceed

${steps.map((step, index) => `${index + 1}. ${step}`).join('\n')}

If you have any trouble you can get support in multiple ways:
* by sending email to the [dev mailing list](mailto:dev@pulsar.apache.org) ([subscribe](mailto:dev-subscribe@pulsar.apache.org))
* on the [#dev channel on Pulsar Slack](https://apache-pulsar.slack.com/channels/dev) ([join](https://pulsar.apache.org/community#section-discussions))
* in apache/pulsar [GitHub discussions Q&A](https://github.com/apache/pulsar/discussions/categories/q-a)
`;
}

module.exports = async ({ github, context, core }) => {
  const eventPullRequest = context.payload.pull_request;
  if (!eventPullRequest) {
    core.info(`The '${context.eventName}' event isn't a pull request event, skipping the check.`);
    return;
  }
  const { owner, repo } = context.repo;
  const number = eventPullRequest.number;
  const label = process.env.READY_TO_TEST_LABEL || DEFAULT_READY_TO_TEST_LABEL;
  const trunkBranches = parsePatterns(process.env.TRUNK_BRANCHES || DEFAULT_TRUNK_BRANCHES);

  // The event payload is a snapshot of the pull request from the time the workflow run was triggered.
  // Refresh the state so that re-running the workflow picks up changes made after that, such as
  // adding the label or marking the pull request as ready for review.
  const { data: pullRequest } = await github.rest.pulls.get({ owner, repo, pull_number: number });

  if ((pullRequest.labels || []).some(prLabel => prLabel.name === label)) {
    core.info(`Found the '${label}' label on #${number}.`);
    return;
  }
  core.info(`There is no '${label}' label on #${number}.`);

  // Each blocker explains why the CI didn't run and what to do about it. The remedies are rendered as
  // the first steps of the instructions so that they match the checks which actually failed.
  const stackRemedy = 'Wait for this pull request to reach the bottom of the stack: once the pull '
    + 'requests below it have been merged and the stack has been synced, it targets the trunk branch '
    + 'and its CI runs in apache/pulsar.';
  const blockers = [];
  if (pullRequest.draft) {
    blockers.push({
      reason: 'The pull request is a **draft**, so it isn\'t ready to be reviewed and tested yet.',
      remedy: 'Mark the pull request as ready for review once it is ready to be tested and reviewed.'
    });
  }

  const stack = await resolveStack({ github, core, owner, repo, number });
  if (stack.available && stack.inStack) {
    const positionText = stack.position != null
      ? `entry ${stack.position} of ${stack.size} in stack #${stack.number}`
      : `part of stack #${stack.number}`;
    core.info(`#${number} is ${positionText}.`);
    if (!stack.isBottom) {
      const bottom = stack.bottomPullRequest;
      const bottomLink = bottom ? `, [#${bottom.number}](${bottom.url}),` : '';
      blockers.push({
        reason: `The pull request is ${positionText}. Only the pull request at the bottom of the `
          + `stack${bottomLink} which targets the \`${stack.trunkBranch}\` branch runs the CI in `
          + `apache/pulsar.`,
        remedy: stackRemedy
      });
    }
  } else if (!matchesAnyPattern(pullRequest.base.ref, trunkBranches)) {
    // Not resolved as a GitHub stack: a pull request that targets a branch which isn't a trunk branch
    // is a dependent pull request stacked on top of another one.
    core.info(`#${number} targets the '${pullRequest.base.ref}' branch which isn't a trunk branch.`);
    blockers.push({
      reason: `The pull request targets the \`${pullRequest.base.ref}\` branch instead of a trunk `
        + `branch (${trunkBranches.map(pattern => `\`${pattern}\``).join(', ')}), so it is stacked on `
        + `top of another pull request. Only the pull request at the bottom of a stack runs the CI in `
        + `apache/pulsar.`,
      remedy: stackRemedy
    });
  }

  if (blockers.length === 0) {
    core.info(`#${number} is ready for running the CI.`);
    return;
  }

  await core.summary
    .addRaw(renderSummary({ pullRequestUrl: eventPullRequest.html_url, blockers, label }))
    .write();
  core.setFailed(`#${number} isn't ready for running the CI in ${owner}/${repo}. `
    + `See the job summary for instructions on how to proceed.`);
};
