# Coding Guidelines

_This is a still work-in-progress and is likely to be updated during the early phases of the development of this repository._

This document contains the coding guidelines we follow in this repository. We follow the guidelines here strictly, so make sure your Pull Requests abide by them.

To make it easier for developers to know the guidelines for the type of their contribution, this document has multiple sections. Use the list below to jump to the section related to the code you plan to submit.

## Table of Contents

- [Bash Scripts](#bash-scripts)
- [Python Scripts](#python-scripts)
- [Docker](#docker)

## Bash Scripts

For Bash scripts, we use [ShellCheck](https://www.shellcheck.net/) to help the developers discover common bugs and bad practices related to Bash scripts. We have GitHub workflows that execute ShellCheck on every Bash script in the repository, and fails if the code breaks any of its rules. To make it easier for the developer to test their code before publishing a PR, we have pre-commit hooks that automatically test your code. However, you need to setup `pre-commit` for the hooks to run. Check the README files for instructions.

## Python Scripts

For Python scripts, we follow [PEP8](https://peps.python.org/pep-0008/). Additionally, we use Flask8 rules. Failure to comply by these will result in your PR failing our GitHub workflows. Like Bash scripts, to make it easier for the developer to test their code before publishing a PR, we have pre-commit hooks that automatically test your code. However, you need to setup `pre-commit` for the hooks to run. Check the README files for instructions.

## Docker

For Dockerfile bootstrapping, don't add your code to the Dockerfile directly. Instead, create a Bash script under the bootstrap/ folder. Follow these rules when creating a new bootstrapping file:

1. Make sure the file name starts with a 3-digit number that indicates its order of execution.
2. Keep your files as small as possible (but not smaller!). This way you better employ Docker caching and reduce the number of unnecessary rebuilds.
3. If you need a system package in your bootstrap file, install it at the beginning and remove it at the end. For eaxmple, if you need to download a file using `wget` then do a `dnf install` at the beginning and a `dnf remove` at the end. This keeps the bootstrap files self-contained, and avoid leaving unnecessary system packages in the final Docker image.
   - Don't worry about removing a package that is actaully needed in the final image. There is a step at the end that will do that.
   - Don't worry about a certain DNF package being installed and removed multiple times during the bootstrapping process. Keeping bootstrapping files self-contained and avoiding leaving unnecessary packages is more important than the couple of seconds you will save optimizing the installation of system packages, especially considering Docker caching which means that steps are rarely repeated (assuming a well-written Dockerfile)
