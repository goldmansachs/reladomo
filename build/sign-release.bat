setlocal

set VER=17.0.2

cd ../target
gpg -ab reladomo-%VER%-javadoc.jar
gpg -ab reladomo-%VER%-sources.jar
gpg -ab reladomo-%VER%.jar
gpg -ab reladomo-%VER%.pom
gpg -ab reladomo-gen-util-%VER%-javadoc.jar
gpg -ab reladomo-gen-util-%VER%-sources.jar
gpg -ab reladomo-gen-util-%VER%.jar
gpg -ab reladomo-gen-util-%VER%.pom
gpg -ab reladomo-test-util-%VER%-javadoc.jar
gpg -ab reladomo-test-util-%VER%-sources.jar
gpg -ab reladomo-test-util-%VER%.jar
gpg -ab reladomo-test-util-%VER%.pom
gpg -ab reladomo-serial-%VER%-javadoc.jar
gpg -ab reladomo-serial-%VER%-sources.jar
gpg -ab reladomo-serial-%VER%.jar
gpg -ab reladomo-serial-%VER%.pom
gpg -ab reladomo-xa-%VER%-javadoc.jar
gpg -ab reladomo-xa-%VER%-sources.jar
gpg -ab reladomo-xa-%VER%.jar
gpg -ab reladomo-xa-%VER%.pom
gpg -ab reladomogen-%VER%-javadoc.jar
gpg -ab reladomogen-%VER%-sources.jar
gpg -ab reladomogen-%VER%.jar
gpg -ab reladomogen-%VER%.pom
