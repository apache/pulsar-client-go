This guide illustrates how to perform a release for Apache pulsar-client-go.

In general, you need to perform the following steps:

1. Create a release branch.
2. Update the version and tag of a package.
3. Write a release note.
4. Run a vote.
5. Promote the release.
6. Update the release note.
7. Announce the release.

## Steps in detail

1. Create a release branch.

This example creates a branch `branch-0.X.0` based on `master` where tags will be generated and new fixes will be applied as parts of the maintenance for the release. `0.X.0` is the version of the release.

The branch needs only to be created when creating a minor release rather than a patch release.

Eg: When creating `0.1.0` release, will be creating the branch `branch-0.1.0`, but for `0.1.2` we would keep using the old `branch-0.1.0`.

In these instructions, I'm referring to an fictitious release `0.X.0`. Change the release version in the examples accordingly with the real version.

It is recommended to create a fresh clone of the repository to avoid any local files interfering in the process:

```
git clone git@github.com:apache/pulsar-client-go.git
cd pulsar-client-go
git checkout -b branch-0.X.0 origin/master
```

2. Update the version and tag of a package.

Update the information of the new release to the `VERSION` file and `stable.txt` file and send a PR for requesting the changes.

During the release process, you can create a "candidate" tag which will get promoted to the "real" final tag after verification and approval.

```
# Commit 
$ git commit -m "Release 0.X.0" -a

# Create a "candidate" tag
$ git tag -u $USER@apache.org v0.X.0-candidate-1 -m 'Release v0.X.0-candidate-1'

# Push both the branch and the tag to Github repo
git push origin branch-0.X.0
git push origin v0.X.0-candidate-1
```

3. Write a release note.

Check the milestone in GitHub associated with the release. 

In the released item, add the list of the most important changes that happened in the release and a link to the associated milestone, with the complete list of all the changes. 

4. Run a vote.

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

Please download these packages and review this release candidate:

- Review release notes
- Download the source package and follow the README.md to build and run the pulsar-client-go.

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

Source file:
https://github.com/apache/pulsar/releases/tag/v0.X.0

The tag to be voted upon:
v0.X.0
https://github.com/apache/pulsar-client-node/releases/tag/v0.X.0
```

The vote should be open for at least 72 hours (3 days). Votes from Pulsar PMC members will be considered binding, while anyone else is encouraged to verify the release and vote as well.

If the release is approved here, we can then proceed to the next step.

5. Promote the release.

```
$ git checkout branch-0.X.0
$ git tag -u $USER@apache.org v0.X.0 -m 'Release v0.X.0'
$ git push origin v0.X.0
```

6. Update the release note.

Add the release note to [Pulsar Client Go](https://github.com/apache/pulsar-client-go/releases)

7. Announce the release.

Once the release process is available , you can announce the release and send an email as below:

```
To: dev@pulsar.apache.org, users@pulsar.apache.org, announce@apache.org
Subject: [ANNOUNCE] Apache Pulsar 2.X.0 released

The Apache Pulsar team is proud to announce Apache Pulsar version 2.X.0.

Pulsar is a highly scalable, low latency messaging platform running on
commodity hardware. It provides simple pub-sub semantics over topics,
guaranteed at-least-once delivery of messages, automatic cursor management for
subscribers, and cross-datacenter replication.

For Pulsar release details and downloads, visit:

http://pulsar.apache.org/en/download/

Release Notes are at:
http://pulsar.apache.org/en/pulsar-client-go-release-notes/

We would like to thank the contributors that made the release possible.

Regards,

The Pulsar Team
```
