#!/bin/bash

RELADOMO_VERSION=17.1.0

echo Enter passphrase:
read -s PASSPHRASE

cd ../target
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-$RELADOMO_VERSION-javadoc.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-$RELADOMO_VERSION-sources.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-$RELADOMO_VERSION.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-$RELADOMO_VERSION.pom
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-gen-util-$RELADOMO_VERSION-javadoc.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-gen-util-$RELADOMO_VERSION-sources.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-gen-util-$RELADOMO_VERSION.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-gen-util-$RELADOMO_VERSION.pom
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-test-util-$RELADOMO_VERSION-javadoc.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-test-util-$RELADOMO_VERSION-sources.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-test-util-$RELADOMO_VERSION.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-test-util-$RELADOMO_VERSION.pom
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-serial-$RELADOMO_VERSION-javadoc.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-serial-$RELADOMO_VERSION-sources.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-serial-$RELADOMO_VERSION.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-serial-$RELADOMO_VERSION.pom
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-xa-$RELADOMO_VERSION-javadoc.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-xa-$RELADOMO_VERSION-sources.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-xa-$RELADOMO_VERSION.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomo-xa-$RELADOMO_VERSION.pom
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomogen-$RELADOMO_VERSION-javadoc.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomogen-$RELADOMO_VERSION-sources.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomogen-$RELADOMO_VERSION.jar
gpg --batch --yes --passphrase $PASSPHRASE -ab reladomogen-$RELADOMO_VERSION.pom
