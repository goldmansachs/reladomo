# Steps to Release Reladomo

## 1. Version Number and Release Guidelines
The Reladomo is released from the HEAD of the master branch. Each release will include all changes merged to the master branch. The releasable code on the HEAD of the branch need to pass the CI/CD build.
The version consist of three numbers for major, minor and bug fix releases. 
The next major release version is associated with non-backward compatible changes or new major features introduction. The minor version is incremented for new features and backward compatible changes. The bug fix release is used to release a fix for an individual bug.
At this point you need to understand what features are planned for release and what version number will be used for the release.

## 2. Fetch the Codebase for the Release 

Update the codebase from the remote git:

    git fetch --all
    
Setup upstream:

    git remote add upstream https://github.com/goldmansachs/reladomo.git
    
Rebase with the master:

    git rebase upstream/master

## 3. Update the Release Notes (CHANGELOG.md)
You need to collect all changes, features and bug fixes to go into the release and list them in the CHANGELOG.md. The list can be formed from the commits in git log. Get the list of the changes with the following command. The version number in the command is the tag from the previous release.
    git log 17.1.0..HEAD --oneline
    <last release tag> is just a version like 16.1.4

Sort the changes on new features, improvements and bug fixes.
Include any major features, rework or not backward compatible changes in the front of the CHANGELOG. 

Edit files sign-release.bat and sign-release.sh.
update version number in each of these files.

Commit changes in CHANGELOG.md, sign-release.bat and sign-release.sh, execute pull request and merge into the master.

## 4. Tag the Release on github

To create a release in github you need to mark the released version of the codebase with version tag. This can be done from the github site or with git CLI.

To tag the release from CLI, execute the following commands. You need to use the upcoming version number in the last command.

    git fetch --all
    git push origin --tags
    git checkout tags/17.1.0
    

## 5. Build the Reladomo Distribution

In the local shell go to the build directory and execute build commands:

    build clean prep-release
    
## 6. Sign the Rrelease with Your GPG Signature
To sign the release you need to have your own PGP key stored in the  .../gnupg/pubring.kbx file protected by your master password. The instruction to create the PGP key are listed in the end of this document.

To sign the release binaries with your signature execute:

    cd build
    sign-release
    build bundle-release
    

## 7. Upload the Release Files to the Nexus Maven Repository
You need to have access to the Reladomo product in the Maven Repository. If you do not have it already, request access with the JIRA ticket (similar to this: [https://issues.sonatype.org/browse/OSSRH-50072](https://issues.sonatype.org/browse/OSSRH-50072 "https://issues.sonatype.org/browse/OSSRH-50072")).

With the access to the repository in place:

- Login into the Nexus web site [https://oss.sonatype.org/](https://oss.sonatype.org/ "https://oss.sonatype.org/").
- click on "Staging upload" menu item on the left.
- choose upload mode: artifact bundle.
- upload each of 6 bundle-* files from the target directory.
- open "Staging Repositories" tab and scroll to the bottom of the list. You should see the six upladed bundless at the end of the list. Make sure that all 6 are "closed", which means the upload and verification went well.
- if the bundles are not closed, you need to delete the bundles, fix the problem and re-uplaod them again.
- open the "staging upload" tab and upload each of these six bundles* files from the target directory.
- check checkbox next to each bundle and click on the "Release" button.

## 8. Confirm the Release is Available to download from the Maven Repository

## Acquiring the Personal PGP Key
If you do not have the PGP key, you can acquire it by:

- install gpg4win

- generate key with command
`gpg --default-new-key-algo rsa4096 --gen-key`

- send the key to the mother-ship:
`gpg --send-keys DEA923843928474739473743749841`
 
- give 15 minutes to the system to propagate the key globally.
- 
- store the master password for your signature in safe place. You will not be able to use it if the master password is lost.
