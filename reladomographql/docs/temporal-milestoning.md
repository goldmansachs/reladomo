# Temporal Chaining (Milestoning)
Reladomo works very well with the [temporally chained](https://goldmansachs.github.io/reladomo-kata/reladomo-tour-docs/tour-guide.html#N408B5) 
(milestoned) database schemas.
The temporal and bitemporal chaining can be expressed in the corresponding GraphQL queries and mutations.
For example to query `processingDate` temporally milestoned objects you can call query: 

```graphql
query {
  accounts (findMany: {processingDate: {eq: "2019-10-13T04:04:39+00:00"}})
  {
    accountNum
    processingDateFrom
    processingDateTo
  }
}

```
Here is result. Note how the processingDate input belongs to the interval of [processingDateFrom, processingDateTo) of the data.

```json
{
  "data": {
    "accounts": [
      {
        "accountNum": "100002",
        "processingDateFrom": "2019-01-03T08:00:00Z",
        "processingDateTo": "INFINITY"
      }
    ]
  }
} 
``` 

Same approach can be applied to the bitemporal milestoned data. For example:


```graphql
query {
  balances (findMany: {processingDate: {eq: "2019-10-13T04:04:39+00:00"} and: {businessDate: {eq: "2019-10-12"}}})
  {
    accountNum
    processingDateFrom
    processingDateTo
  }
}
```

will result in:
```json
{
  "data": {
    "balances": [
      {
        "accountNum": "100001",
        "processingDateFrom": "2010-01-03T08:00:00Z",
        "processingDateTo": "INFINITY"
      },
      {
        "accountNum": "100002",
        "processingDateFrom": "2010-01-03T08:00:00Z",
        "processingDateTo": "INFINITY"
      },
      {
        "accountNum": "100003",
        "processingDateFrom": "2010-01-03T08:10:00Z",
        "processingDateTo": "INFINITY"
      }
    ]
  }
}
```
The query supports timestamps in:
* [RFC3339](https://tools.ietf.org/html/rfc3339) / [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) formats. i.e. "2019-12-09T20:55:45Z"
* The business date can be presented as a timestamp or a date in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format. i.e. "2019-12-09"
* The as-of attributes defining `businessDate` and `processingDate` represent time interval with specifically defined `INFINITY`.
 The infinity has to be specified as `"INFINITY"`in query and will be presented as `"INFINITY"` in result. 
* If the query for temporally milestoned objects does not specify processingDate, the system will automatically query the currently open records 
(the time intervals are open to `INFINITY`).
* If the object defines business date as-of attribute, it has to be included in the query.
Try this query to see how the implicit milestoning works:
```graphql
query {
balances (findMany: {value: {greaterThan: 100}})
  {
    accountNum
    value
  }
}
```

#Mutation with Temporal Milestoning
The mutation are explained in [Mutations](mutations.md). The temporally milestoned objects with Business Date need to know the business date when inserted into the DB.
For this reason, you need to explicitly state `businessDate` in the mutation request. For example:

```graphql
mutation { testBalance_insert(
    businessDate: "2020-04-12"
    testBalance: {productId: 73 accountId: "324" positionType: 3}) {
    productId
    accountId
    positionType
    businessDateFrom
    businessDateTo
    processingDateTo 
}}
``` 
