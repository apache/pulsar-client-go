This guide illustrates how to perform a release for Apache pulsar-client-go.

In general, you need to perform the following steps:

1. Create a release branch.
2. Update the version and tag of a package.
3. Build and inspect an artifact.
4. Sign and stage the artifacts.
5. Write a release note.
6. Run a vote.
7. Promote the release.
8. Update the release note.
9. Announce the release.

### Requirements
- [Creating GPG keys to sign release artifacts](https://pulsar.apache.org/contribute/create-gpg-keys/)

## Steps in detail

1. Create a release branch.

This example creates a branch `branch-0.X.0` based on `master` where tags will be generated and new fixes will be applied as parts of the maintenance for the release. `0.X.0` is the version of the release.

The branch needs only to be created when creating a minor release rather than a patch release.

Eg: When creating `0.1.0` release, will be creating the branch `branch-0.1.0`, but for `0.1.2` we would keep using the old `branch-0.1.0`.

In these instructions, I'm referring to an fictitious release `0.X.0`. Change the release version in the examples accordingly with the real version.

It is recommended to create a fresh clone of the repository to avoid any local files interfering in the process:

```
git clone https://github.com/apache/pulsar-client-go.git
cd pulsar-client-go
git checkout -b branch-0.X.0 origin/master
```

2. Update the version and tag of a package.

Update the information of the new release to the `VERSION` file and `stable.txt` file and send a PR for requesting the changes.

During the release process, you can create a "candidate" tag which will get promoted to the "real" final tag after verification and approval.

```
# Create a "candidate" tag
export GPG_TTY=$(tty)
git tag -u $USER@apache.org v0.X.0-candidate-1 -m 'Release v0.X.0-candidate-1'

# Push both the branch and the tag to Github repo
git push origin branch-0.X.0
git push origin v0.X.0-candidate-1
```

3. Build and inspect an artifact.

Generate a release candidate package.

```bash
$ tar -zcvf apache-pulsar-client-go-0.X.0-src.tar.gz .
```

4. Sign and stage the artifacts 

The src artifact need to be signed and uploaded to the dist SVN repository for staging.

```
$ gpg -b --armor apache-pulsar-client-go-0.X.0-src.tar.gz
$ shasum -a 512 apache-pulsar-client-go-0.X.0-src.tar.gz > apache-pulsar-client-go-0.X.0-src.tar.gz.sha512 
```

Checkout repo for uploading artifacts
```
$ svn co https://dist.apache.org/repos/dist/dev/pulsar pulsar-dist-dev
$ cd pulsar-dist-dev
```

Create a candidate directory at the root repo
```
$ svn mkdir pulsar-client-go-0.X.0-candidate-1
$ cd pulsar-client-go-0.X.0-candidate-1
```

Copy the signed artifacts into the candiate directory and commit
```
$ cp ../apache-pulsar-client-go-0.X.0-* .
$ svn add *
$ svn ci -m 'Staging artifacts and signature for Pulsar Client Go release 0.X.0-candidate-1'
```

Since this needs to be merged in master, we need to follow the regular process and create a Pull Request on GitHub.

5. Write a release note and update `CHANGELOG.md`.

Check the milestone in GitHub associated with the release. 

In the released item, add the list of the most important changes that happened in the release and a link to the associated milestone, with the complete list of all the changes. 

6. Run a vote.

Send an email to the Pulsar Dev mailing list:

```
To: dev@pulsar.apache.org
Subject: [VOTE] Pulsar Client Go Release 0.X.0 Candidate 1

Hi everyone,
Please review and vote on the release candidate #1 for the version 0.X.0, as follows:
[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

This is the first release candidate for Apache Pulsar Go client, version 0.X.0.

It fixes the following issues:
https://github.com/apache/pulsar-client-go/milestone/1?closed=1

Pulsar Client Go's KEYS file contains PGP keys we used to sign this release:
https://downloads.apache.org/pulsar/KEYS

Please download these packages and review this release candidate:
- Review release notes
- Download the source package (verify shasum, and asc) and follow the
README.md to build and run the pulsar-client-go.

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

Source file:
https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-go-0.X.0-candidate-1/

The tag to be voted upon:
v0.X.0
https://github.com/apache/pulsar-client-go/tree/v0.X.0-candidate-1

SHA-512 checksums:
97bb1000f70011e9a585186590e0688586590e09  apache-pulsar-client-go-0.X.0-src.tar.gz
```

The vote should be open for at least 72 hours (3 days). Votes from Pulsar PMC members will be considered binding, while anyone else is encouraged to verify the release and vote as well.

If the release is approved here, we can then proceed to the next step.

7. Promote the release.

```
$ git checkout branch-0.X.0
$ export GPG_TTY=$(tty)
$ git tag -u $USER@apache.org v0.X.0 -m 'Release v0.X.0'
$ git push origin v0.X.0
```

Promote the artifacts on the release location (need PMC permissions):

```
svn move -m "release 0.X.0" https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-go-0.X.0-candidate-1 \
         https://dist.apache.org/repos/dist/release/pulsar/pulsar-client-go-0.X.0
Remove the old releases (if any). We only need the latest release there, older releases are available through the Apache archive:
```

# Get the list of releases
```
svn ls https://dist.apache.org/repos/dist/release/pulsar | grep client-go
```

# Delete each release (except for the last one)

```
svn rm https://dist.apache.org/repos/dist/release/pulsar/pulsar-client-go/pulsar-client-go-0.X.0
```

8. Update the release note.

Add the release note to [Pulsar Client Go](https://github.com/apache/pulsar-client-go/releases)

9. Announce the release.

Once the release process is available , you can announce the release and send an email as below:

```
To: dev@pulsar.apache.org, users@pulsar.apache.org, announce@apache.org
Subject: [ANNOUNCE] Apache Pulsar Go Client 0.X.0 released

The Apache Pulsar team is proud to announce Apache Pulsar Go Client version 0.X.0.

Pulsar is a highly scalable, low latency messaging platform running on
commodity hardware. It provides simple pub-sub semantics over topics,
guaranteed at-least-once delivery of messages, automatic cursor management for
subscribers, and cross-datacenter replication.

For Pulsar release details and downloads, visit:
https://github.com/apache/pulsar-client-go/releases/tag/v0.x.0

Release Notes are at:
https://github.com/apache/pulsar-client-go/blob/master/CHANGELOG.md

We would like to thank the contributors that made the release possible.

Regards,

The Pulsar Team
```
