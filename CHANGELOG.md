# Change Log
## 16.1.2 - 2016-10-18 
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
