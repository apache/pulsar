# Pulsar Documentation Label Guide

> 👩🏻‍🏫 **Summary**
> 
> This guide instructs you on how to label your doc PR.

When submitting an issue or PR, you must [provide doc label information](https://github.com/apache/pulsar/blob/master/.github/PULL_REQUEST_TEMPLATE.md#documentation) by **selecting the checkbox**, so that the Bot can label the PR correctly. 

Label name|Usage
|---|---
`doc-required`|Use this label to indicate this issue or PR impacts documentation.<br><br> **You have not updated the docs yet**. The docs will be submitted later. 
`doc-not-needed`| The code changes in this PR do not impact documentation.
`doc`|This PR contains changes that impact documentation, **no matter whether the changes are in markdown or code files**.
`doc-complete`|Use this label to indicate the documentation updates are complete.
`doc-label-missing`|The Bot applies this label when there is no doc label information in the PR if one of the following conditions is met:. <br><br> - You do not provide a doc label. <br><br> - You provide multiple doc labels. <br><br> - You delete backticks (``) in doc labels. <br>For example,<br> [x] `doc-required` ✅ <br> [x] doc-required ❌ <br><br> - You add blanks in square brackets. <br>For example, <br> [x] `doc-required` ✅ <br>[ x ] `doc-required` ❌

## References

For more guides on how to make contributions to Pulsar docs, see [Pulsar Documentation Contribution Overview](./../README.md).