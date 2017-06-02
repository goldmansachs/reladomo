# Change Log
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

# Change Log
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
