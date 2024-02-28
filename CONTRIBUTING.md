# Contribution Guidelines

Thanks for your interest in contributing to Montecristo.

Below you'll find some guidelines to help you get started.
There are multiple ways you can contribute to the project, you can open Pull Requests
to fix a bug or add a new feature, report issues or improve the rules.

Montecristo is written in Kotlin, and built using gradle - it follows the standard structure in terms of src/main etc.

- [Code Contributions](#code-contributions)
    - [Running Unit Tests](#running-unit-tests)
    - [Debugging](#debugging)

## Code Contributions

---
**Note:** If you'll contribute code or examples via a Pull Request (PR) to this project, be sure to first read
the Contribution License Agreement (CLA) at https://cla.datastax.com/. You'll be required to sign it with your GitHub
account before we can consider accepting your PR changes.

---

**Before you start working on a feature or major change, we ask you to discuss your idea with the members
of the community by opening an issue in GitHub, this way we can discuss design considerations and
feasibility before you invest your time on the effort.**

When  you're ready to code, follow these steps:

1. If you haven't done so yet, go ahead now and fork the Montecristo repository into your own Github account and clone it
   on your machine. That's how you'll be able to make contributions, you cannot push directly to the main repository, but
   you can open a Pull Request for your branch when you're ready.


2. From your fork, create a new local branch where you'll make changes. Give it a short, descriptive, name.
   If there's an associated issue open, include its number as a prefix, *#issue-name*, for example: 10-fix-bug-xyz.


3. Make the changes you want locally and validate them with the [test tools](#running-unit-tests) provided. Push the
   updated code to the remote branch in your fork.


4. Finally, when you're happy with your edits, open a Pull Request through the GitHub UI.


5. The automated tests in GitHub Actions will kick in and in a few minutes you should
   have some results. If any of the checks fail, take a look at the workflow logs to understand
   what happened. You can push more commits to the PR until the problem is resolved, the checks will run again.


6. Once all checks are passing, your PR is ready for review and the members of the Montecristo community will
   start the process. You may be asked to make further adjustments, this is an iterative process, but
   in the end if everything looks good, we will be able to merge the PR to the main branch.

   
### Running Unit Tests

The main tests of the project are located under the montecristo folder. These tests are meant to run very quickly with no external dependencies.

To run the unit tests, go the montecristo folder within the project in a terminal window and execute the following command:

> $ ./gradlew test

It will show the tests run within the console and if any test fail, it will report the failure.

Make sure you add tests to your PR if you're making a major contribution.

### Debugging

There are two ignored tests which are present in the code for the purposes of debugging Montecristo against complete tarballs. In the test/kotlin/com.datastax.montecristo/commands folder, you will find 2 test classes.

Each of these can be quickly configured to point to a folder with the results of a diagnostic collection.
* GenerateDiscoveryReportTest - this is used to run the report generation part, and will parse all files (except logs and JMX) in providing output.
* GenerateMetricsDBTest - this is used to test the parsing / loading of the JMX metrics and log parsing for the lucene search index.
  
To use these in debugging, remove the @Ignore annotation and change the jira variable to the correct path.

