# Building Reladomo

Reladomo is built from the command line with the `build/build.sh` or `build/build.bat` files.
Reladomo is currently built with JDK 1.8.
Reladomo cannot be built with any other version of the JDK.

## Setting up your environment
You'll need to clone the repository and have JDK 1.8 installed. You can then setup your environment.

If you're using `build.bat` under Windows, you must modify the first line of `build/setenv.bat` to point to 
your JDK installation.

If you're using `build.sh`, you can set a variable in your shell, for example:
```
export RELADOMO_JDK_HOME=/opt/jdk1.8.0_202
```
Alternatively, you can modify `build/setenv.sh` by uncommenting the similar line near the top
and setting the value to point to your JDK.

Try the following command to test your setup:
```
build/build.sh compile-reladomo
```

## Reladomo Build Process
The build process will download relevant dependencies and start ant to build Reladomo.
You do not need your own copy of ant or any other jar.

The build takes a target. See the `build/build.xml` file for all the targets. Some of the
important ones are:
* `compile-reladomo`: compiles the main library and code generator.
* `compile-reladomo-test`: compiles Reladomo and all tests.
* `reladomo-test-suite`: compiles and runs the test suite.

### Using a different Maven mirror
The file `build/repos.txt` has the specification for where the dependencies are downloaded from.
If you wish to use your own mirror, make the appropriate changes to that file.

