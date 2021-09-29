---
name: Flaky test
about: Report a flaky test failure
title: 'Flaky-test: [test class].[test method]'
labels: component/test flaky-tests type/bug
assignees: ''
---
<!--- 

Instructions for reporting a flaky test using this issue template:

1. Replace [test class] in title and body with the test class name
2. Replace [test method] in title and body with the test method that failed. Multiple methods are flaky, remove the content that refers to the test method.
3. Replace "url here" with a url to an example failure. In the Github Actions workflow run logs, you can right click on the line number to copy a link to the line. Example of such url is https://github.com/apache/pulsar/pull/8892/checks?check_run_id=1531075794#step:9:377 . The logs are available for a limited amount of time (usually for a few weeks).
4. Replace "relevant parts of the exception stacktrace here" with the a few lines of the stack trace that shows at least the exception message and the line of test code where the stacktrace occurred.
5. Replace "full exception stacktrace here" with the full exception stacktrace from logs. This section will be hidden by default.
6. Remove all unused fields / content to unclutter the reported issue. Remove this comment too.

-->
[test class] is flaky. The [test method] test method fails sporadically.

[example failure](url here)

```
[relevant parts of the exception stacktrace here]
```

<details>
<summary>Full exception stacktrace</summary>
<code><pre>
full exception stacktrace here
</pre></code>
</details>