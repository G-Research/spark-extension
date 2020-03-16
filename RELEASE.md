# Releasing `spark-extension`

This provides instructions on how to release a version of `spark-extension`.

## Releasing from master

Follow this procedure to release a new version:

- Remove the `-SNAPSHOT` prefix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.0-SNAPSHOT` → `1.1.0`
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.0`. Do not push to github yet.
- Tag that commit with a tag message like `Release v1.1.0`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`
  - Bump the version to the next [minor version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` prefix again, e.g. `1.1.0` → `1.2.0-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.0`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

## Releasing a bug-fix version

A bug-fix version needs to be released from a [minor-version branch](https://semver.org/), e.g. `branch-1.0`.

### Create a bug-fix branch

If there is no bug-fix branch yet, create it:

- Create such a branch from the respective [minor-version tag](https://semver.org/), e.g. `v1.0.0`.
- Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` prefix again, e.g. `1.1.0` → `1.1.1-SNAPSHOT`.
- Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.1`.
- Push this commit to origin.

Merge your bug fixes into this branch as you would normally do for master, use PRs for that.

### Release from a bug-fix branch

This is very similar to [releasing from master](#releasing-from-master),
but the version increment occurs on [patch level](https://semver.org/):

- Remove the `-SNAPSHOT` prefix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.1-SNAPSHOT` → `1.1.1`
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.1`. Do not push to github yet.
- Tag that commit with a tag message like `Release v1.1.1`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`
  - Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` prefix again, e.g. `1.1.1` → `1.1.2-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.2`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.
