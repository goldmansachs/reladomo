# Reladomo

## What is it? 
Reladomo is an object-relational mapping framework with the following enterprise features:

* Strongly typed compile-time checked query language 
* Bi-temporal chaining 
* Transparent multi-schema support 
* Full support for unit-testable code 

## What can I do with it? 
* Model data as objects with meaningful relationships between them 
* Define classes and relationships using simple XML files 
* Traverse, query, fetch, and update graphs of objects in an idiomatic object-oriented way 
* Manage bi-temporal data using built-in methods 
* Define, create, query, and  update data that has both business date and processing date axes 
* Maintain complete and accurate audit history of changes efficiently 
* Answer as-of questions such as "what did this object look like at the end of last quarter" 
* Build applications as diverse as interactive web-apps to batch-processing 
* Leverage transactions and batch operations to support high-performance throughput 
* Detach objects to allow users to change data off-line 
* Write database vendor-independent code 


## Detailed feature list 
* Strongly typed compile-time checked query language 
* Audit-only, Business time-series only, and Bi-temporal chaining 
* Transparent multi-schema support (partition data across many databases) 
* Object-oriented batch operations 
* Flexible object relationship inflation 
* Detached objects (allow data to be changed independently (a.k.a. delayed edit functionality) of the DB and then pushed (or reset) as and when required) - useful when users are editing data in a GUI form
* Multi-Threaded matcher Loader (MTLoader) is a high-performance pattern for merging changes from another source (file, feed, other DB, etc.) to your existing DB data. By design it is flexible/customizable and re-runnable 
* Tunable caching by object type - partial, full, full off-heap
* Available meta-data - enables higher-level programming paradigms
* Multi-tier operation - obviates the need for direct DB access from client-side apps, enables better connection sharing, with no code changes required
* Full support for unit-testable code 
* Databases supported include: Sybase (ASE & IQ), DB2, Oracle, Postgres, MS-SQL, H2, Derby, "generic" ...

## Sample Project
To help getting started with Reladomo, a simple project is available with maven and gradle build set-up.

Prerequisite: install maven or gradle.

```
git clone https://github.com/goldmansachs/reladomo.git
cd samples/reladomo-sample-simple
```

#### Maven
```
mvn clean install
```

#### Gradle
```
gradle clean build
```

Once build is successful, run `src/main/java/sample/HelloReladomoApp` to see how it behaves.


## References

There are number of references available within [Reladomo Javadoc jar file](http://search.maven.org/remotecontent?filepath=com/goldmansachs/reladomo/reladomo/16.0.0/reladomo-16.0.0-javadoc.jar).
Extract the jar file and refer to the docs below.

| Reference        | Description           | File Path  |
| ------------- |-------------| -----|
| Tutorial| This tutorial demonstrates the necessary steps to get your Reladomo project started. | userguide/ReladomoTutorial.html |
| FAQ      | Reladomo FAQ      |   mithrafaq/ReladomoFaq.html |
| Reladomo Test Resource | This document explains the steps required to use Reladomo objects in unit tests.     |   mithraTestResource/ReladomoTestResource.html |
| Reladomo Notification | When you have multiple JVMs connecting to a DB via Reladomo, you need to keep each JVM up-to-date with changes made by any of the other JVMs. Reladomo Notification is the primary mechanism for achieving this and keeping each JVMs Reladomo cache fresh.     |   notification/Notification.html |
| Reladomo Primary Key Generator | Primary key generator is an optional feature in Reladomo that allows Reladomo objects to declare how the primary key is going to be generated. | primaryKeyGenerator/PrimaryKeyGenerator.html |
| Reladomo Database Definition Generators | Database definition language (DDL) file generation is an optional feature in Reladomo that allows users to generate scripts to create tables, indices and foreign keys from the Reladomo object definition XML files. | mithraddl/ReladomoDdlGenerator.html |
| Reladomo Object XML Generator | To expedite the creation of object XML files from existing schema, an object XML file generator has been created. It connects directly to a database, retrieving a list of the existing tables and generating object XML files that appropriately map to these tables. | objectxmlgenerator/Generator.html |
| Visualize Domain Model Using Reladomo Metadata | When a persistent set of objects is specified in Reladomo metadata, the objects can be visualized. The output can be used as documentation, or simply browsed through to gain understanding of the domain. | visualization/ReladomoVisualization.html |
| Reladomo Architecture | Reladomo internal architecture. | architecture/ReladomoInternalArchitecture.html |
| Presentations | Reladomo presentation materials. | presentations |

