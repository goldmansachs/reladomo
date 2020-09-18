# Aggregation

Aggregation allows to combine multiple records and present the combined value of the numeric columns.
Let's say we have a table with positions:

| accountNum | businessDate | product | value |
|------------|--------------|---------|-------|
| 100002     | 2019-10-12   | IBM     | 100.0 |
| 100001     | 2019-10-12   | IBM     | 200.0 |
| 100003     | 2019-10-12   | IBM     | 500.0 |
| 100002     | 2019-10-12   | APPL    | 100.0 |
| 100002     | 2019-10-12   | GOOG    | 100.0 |
| 100002     | 2019-10-12   | FB      | 100.0 |
| 100002     | 2019-10-12   | GS      | 300.0 |

It can be aggregated to show one row per account with the sum of values for this account:

| accountNum | value (sum) |
|------------|-------------|
| 100002     | 700.0       |
| 100001     | 200.0       |
| 100003     | 500.0       |

The aggregated queries are supported in Reladomo GraphQL. The aggregate queries refer to the table with _aggregate sufix.
Aggregate queries are defined bu the group by attribute (i.e. accountNum) and the mathimatical function applied to the
numeric values (i.e. value {sum} ). For example:

```graphql
query { 
  balance_aggregate (filter: {businessDate: {eq: "2019-10-12"}}) 
  { 
    accountNum
    value { sum count }
  }
}
```

Will return result like this:
```json
{
  "data": {
    "balance_aggregate": [
      {
        "accountNum": "100003",
        "value": {"sum":500.0,"count":1.0}
      },
      {
        "accountNum": "100002",
        "value": {"sum":700.0,"count":5.0}
      },
      {
        "accountNum": "100001",
        "value": {"sum":200.0,"count":1.0}
      }
    ]
  }
}
```

Reladomo completes aggregation queries very fast. Let's try this query on a million of a rows
```graphql
 query {
   pureBalance_aggregate (filter: {businessDate: {eq: "2019-10-12"} and: {value: {greaterThan: 2000}}})
  {
    accountNum
    value { sum }
  }
}
```

The aggregate query can traverse relationships in the QL as well as in the result set. Here is an example:
```graphql
query {
  pureBalance_aggregate (filter: {businessDate: {eq: "2019-10-12"} and: {account: {country: {notEq: "Atlanta"}}}})
 {
   account
   {
      accountNum
   }
   value { sum count }
 }
}

```
Besides calculating the sum of the record values, the aggregation also can use the following math operations:
* sum
* max
* min
* mean
* count (count of aggregated rows)
* standardDeviationSample
* standardDeviationPopulation
* varianceSample
* variancePopulation

The GraphQL aggregate queries are translated into [Reladomo Aggregate Lists]((https://goldmansachs.github.io/reladomo/presentations/mithraAdvancedUseCases/ReladomoAdvancedUseCases.xhtml#(11))


