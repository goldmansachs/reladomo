# Change Log
## 18.1.0 - 2022-02-06
### Enhancements:
- Add MariaDB support to database definition generator
- Auto quote columns that are SQL keywords or have spaces
    - Quoted identifiers are now forced to "on" at connection time to Sybase and MS SQL Server
- Update to H2 2.1.210
    - This was a bigger change than expected. H2 2.x behaves quite differently from 1.x
            - It's a lot more strict regarding identifiers. This may cause issues with table names in unit tests.
            - Numerical computations can result in slightly different outcomes (e.g. rounding down vs up).
    - For unit tests, `MODE=LEGACY` is auto set for the in memory instance. The main use of this is the `IDENTITY()` function.
### Bug Fixes:
- Fix parsing large long values in test data files

## 18.0.0 - 2020-09-06
### Enhancements:
- Switch to jdk8. Reladomo's mininum supported jdk is now JDK 8
- Remove GSC collection. Eclipse collections is now the only supported part of the interface.
- Initial implementation of GraphQL API
- Change Strings to be quoted in the toString() representation of in-clause operations.
- Only run off heap free thread when necessary.

### Bug Fixes:
- Fix parameter tokenization with comma
- Correct limit row count for MariaDB and Postgres.
- Implement equals and hashCode() on Timestamp-part and Date-part Calculators.
- Fix a bug in wild card expression parsing

## 17.1.4 - 2019-11-28
### Bug Fixes:
- Fix json deserialization for primitive attributes with "null"

## 17.1.3 - 2019-10-16
### Bug Fixes:
- Fix json deserialization for to-many relationships

# Change Log
## 17.1.2 - 2019-09-19
### Bug Fixes:
- Fix json deserialization with nulls
- Fix ArrayIndexOutOfBoundsException in MultiExtractorHashStrategy

## 17.1.1 - 2019-09-01
### Bug Fixes:
- Fix json deserialization without relationships

## 17.1.0 - 2019-07-16
### Enhancements:
- Add support for test file charset
- Changed handling of the optimistic lock exception to support situations where the underlying data has duplicate record for the given milestone. In this case a MithraUniqueIndexViolationException exception will be trown and no retries will be made.
- Better error message during code generation
- Remove duplicated attribute "finalGetter" in type EmbeddedValueType. The same attribute (name and target namespace) is already defined in type NestedEmbeddedValueType

### Bug Fixes:
- Fix deep fetch and query cache timing
- Fix build failure caused by missing OpenJDK 6 dependency on Travis CI (#1)
- Clean Notification Manager Shutdown
- Fix attribute setters in inherited list classes
- Fix code generation for class named Class. covered by: craig_motlin.dco
- Fix superclass + interface combination
- Multi threaded deep fetch exception handling
- Fix the several typos detected by github.com/client9/misspell
- Fix TXRollback for test


## 17.0.2 - 2018-05-10
### Bug Fixes:
- Fix MultiUpdateOperation combine method for increment
- Preserve SerializationConfig's metadata status across builder calls.
- Ignore as-of attributes when determining pass through direction for operations
- Upgrade sample project to use Reladomo 17.0.1.

# Change Log
## 17.0.1 - 2018-03-12
Note: this releases fixes a serious regression introduced in 16.7.0
### Enhancements:
- Add connection max lifetime after start to connection pool

### Bug Fixes:
- Fix transaction batching/reordering (introduced in 16.7.0)
- Fix default Sybase IQ update via insert threshold
- Fix running tests in different timezones

## 17.0.0 - 2018-03-02
### Enhancements:
- Eclipse Collections integration. Please see [migration documentation](https://github.com/goldmansachs/reladomo/blob/master/reladomo/src/doc/GSC_TO_EC_MIGRATION_GUIDE.md)
    - GS collections supported until March 2019
    - Minor backward breaking changes that should not affect most users

## 16.7.1 - 2018-02-28
### Bug Fixes:
- Fix Dated NonUniqueIndex with optimistic locking in a transaction

## 16.7.0 - 2018-02-06
### Enhancements:
- New module: XA integration with a JMS message loop

### Bug Fixes:
- Fix IllegalStateException when shutdown happens from hook
- Fix for Deep fetch issue
- Fix update on objects from different sources
- Fix navigating through a dated list with None operation
- Fix for incrementing past data for bitemporal tables

## 16.6.1 - 2017-10-04
### Enhancements:
- JDK9 compatibility: remove use of jigsawed class

### Bug Fixes:
- Fix NPE in transactional reads of non-dated objects
- Fix multiple-or-clauses in deep relationships sql generation

## 16.6.0 - 2017-09-22
### Enhancements:
- Support Sybase IQ native driver 16.1 with bulk insert support
- Added n/uni char/varchar to MsSQL reverse mapping
- Remove unimplemented Enum mapping

### Bug Fixes:
- Fix DbExtractor NPE with timezone converted values
- Fix interface method visibility

## 16.5.1 - 2017-08-03
### Enhancements:
- Add getInstanceForOracle12 to OracleDatabaseType that can do batch updates with optimistic locking

### Bug Fixes:
- Disable batch updates on Oracle when doing optimistic locking

## 16.5.0 - 2017-07-28
### Enhancements:
- New merge api for transactional lists.
- Add Postgres, MsSql, Maria, and Oracle support to schema to xml generation.

### Bug Fixes:
- Fix over specified relationship resolution.
- Ensure same ordering when processing same input for generation.

## 16.4.0 - 2017-07-17
### Enhancements:
- Add a shutdown hook to the notification manager.
- Improve subquery cache by recognizing more cases
- Use connection based temp tables more frequently, with proper retry

### Bug Fixes:
- Fix code generator dirty checking (CRC)

## 16.3.2 - 2017-07-05
### Bug Fixes:
- Fix equalsEdgePoint with subquery

## 16.3.1 - 2017-07-05
### Enhancements:
- Implement a simple subquery cache.
- Notification initialization may be done after full cache load.

### Bug Fixes:
- Fix simulated sequence rare initialization deadlock.
- Fix NPE in full cache not-exists.
- Prevent compact operations from ending up in the query cache.

## 16.3.0 - 2017-06-02
### Enhancements:
- Better heuristics for camel casing when generating xml from existing schema
- Support nullable boolean in ddl generation and insert/update
- Change generated type for long to bigint for Sybase ASE

## 16.2.0 - 2017-05-25
### Enhancements:
- Serialization/Deserialization utilities with example implementations in Jackson & Gson
    - See the [documentation](https://goldmansachs.github.io/reladomo/serialization/Serialization.html)
- Consolidated class level metadata API
    - See the ReladomoClassMetaData class

### Bug Fixes:
- Honor setting of generated CVS header. Off by default.
- Increment refresh and database retrieval counts for temporal objects correctly.
- Fix NPE in DbExtractor for UTC converted attributes

## 16.1.4 - 2017-05-03
### Enhancements:
- Improve using full cache in a transaction when transaction participation is not required
- Tweak bulk loader connection pooling to reduce connection open/close.
- Add support for bigint for Sybase ASE

### Bug Fixes:
- Fix Sybase inserts with more than 160 columns.

## 16.1.3 - 2017-03-07
### Enhancements:
- Enabled in-memory db extractor merge and Timestamp Attribute time zone conversion
- Reduce db hit with filtered relationship list navigation with deep fetch
- Try harder to resolve mixed partial/full cache queries

### Bug Fixes:
- Allow calls MithraMultithreadedQueueLoader.shutdownPool() to before start
- fix MithraCompositeList.contains

## 16.1.2 - 2017-01-26
This release includes a new document: Reladomo Philosophy & Vision
See the javadoc jar or [online] (https://goldmansachs.github.io/reladomo/)
### Enhancements:
- make sure cache load exceptions are reported at startup
- recognize more DB2 connection dead error codes

### Bug fixes:
- Fix SyslogChecker for String SourceAttribute.
- Fix xml parsing for orderBys.

## 16.1.1 - 2016-11-03
### Enhancements:
- implement equality substitution in chained mapper

## 16.1.0 - 2016-10-18 
### Enhancements:
- multi-update no longer uses or-clauses

### Bug fixes:
- fix combining mapped tuple attributes
- fix Aggregate query as of attribute value setting
- fix findBy and none-cache

## 16.0.0 - 2016-09-13
Initial open source release
### Enhancements:
- Suppressed ClassNotFoundException for notification
- New utility class: MultiThreadedBatchProcessor
