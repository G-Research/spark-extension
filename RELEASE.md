# Releasing Spark Extension

This provides instructions on how to release a version of `spark-extension`. We release this libarary
for a number of Spark and Scala environments, but all from the same git tag. Release for the environment
that is set in the `pom.xml` and create a tag. On success, release from that tag for all other environments
as described below.

Use the `release.sh` script to test and release all versions. Or execute the following steps manually.

## Testing master for all  environments

The following steps release a snapshot and test it. Test all versions listed [further down](#releasing-master-for-other-environments).

- Set the version with `./set-version.sh`, e.g. `./set-version.sh 3.0.0 2.11.12`
- Release a snapshot (make sure the version in the `pom.xml` file ends with `SNAPSHOT`): `mvn clean deploy`
- Test the released snapshot: `./test-release.sh`

## Releasing from master

Follow this procedure to release a new version:

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release.
  Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.0.0] - 2020-03-12`.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.0-SNAPSHOT` → `1.1.0`.
- Update the versions in the `README.md` file to the version of your `pom.xml` to reflect the latest version,
  e.g. replace all `1.0.0-3.1` with `1.1.0-3.1`.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.0`. Do not push to github yet.
- Tag that commit with a version tag like `v1.1.0` and message like `Release v1.1.0`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `./test-release.sh` or the `spark-examples` project for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [minor version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.0` → `1.2.0-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.2.0`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

## Releasing master for other environments

Once you have released the new version, release from the same tag for all other Spark and Scala environments as well:
- Release for these environments, one of these has been released above, that should be the tagged version:
|Spark|Scala|
|:----|:----|
|3.0  |2.12.10 (and 2.13.1)|
|3.1  |2.12.10 (and 2.13.4)|
|3.2  |2.12.15 and 2.13.5|
|3.3  |2.12.15 and 2.13.8|
|3.4  |2.12.16 and 2.13.8|
- Always use the latest Spark version per Spark minor version
- Release process:
  - Checkout the release tag, e.g. `git checkout v1.0.0`
  - Set the version in the `pom.xml` file via `set-version.sh`, e.g. `./set-version.sh 3.0.0 2.12.10`
  - Review the `pom.xml` file changes: `git diff pom.xml`
  - Release the version with `mvn clean deploy`
  - Inspect and test the staged version. Use `./test-release.sh` or the `spark-examples` project for that.
    - If you are happy with everything, release the package with `mvn nexus-staging:release`.
    - Otherwise drop it with `mvn nexus-staging:drop`.
- Revert the changes done to the `pom.xml` file: `git checkout pom.xml`

## Releasing a bug-fix version

A bug-fix version needs to be released from a [minor-version branch](https://semver.org/), e.g. `branch-1.1`.

### Create a bug-fix branch

If there is no bug-fix branch yet, create it:

- Create such a branch from the respective [minor-version tag](https://semver.org/), e.g. create minor version branch `branch-1.1` from tag `v1.1.0`.
- Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.0` → `1.1.1-SNAPSHOT`.
- Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.1`.
- Push this commit to origin.

Merge your bug fixes into this branch as you would normally do for master, use PRs for that.

### Release from a bug-fix branch

This is very similar to [releasing from master](#releasing-from-master),
but the version increment occurs on [patch level](https://semver.org/):

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release.
  Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.0.0] - 2020-03-12`.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.1-SNAPSHOT` → `1.1.1`.
- Update the versions in the `README.md` file to the version of your `pom.xml` to reflect the latest version,
  e.g. replace all `1.0.0-3.1` with `1.1.0-3.1`.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.1`. Do not push to github yet.
- Tag that commit with a version tag like `v1.1.0` and message like `Release v1.1.1`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `spark-examples` for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.1` → `1.1.2-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.2`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

Consider releasing the bug-fix version for other environments as well. See [above](#releasing-master-for-other-environments) section for details.
