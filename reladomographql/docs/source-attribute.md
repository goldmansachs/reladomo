#Source Attribute
Reladomo has the ability to handle objects of the same type stored in tables residing in different databases. 
This is supported via the [SourceAttribute tag](https://goldmansachs.github.io/reladomo/mithrafaq/ReladomoFaq.html#N40680).

To query objects with source attribute you need to specify it in the filter. In this example `acmapCode` is the
 SourceAttribute referring the the database `A`

```graphql
{ tinyBalances (findMany: {acmapCode: {eq: "A"} AND: [{businessDate: {eq: "2002-11-30T23:59:00+00:00"}}] }) {
        balanceId
        acmapCode
        businessDateFrom
        businessDateTo
    }
}
```

The mutation/insert of the objects with source attribute is done by defining the value of the source attribute along
 with the other attributes in the statement:

```graphql
mutation { tinyBalance_insert(businessDate: "2020-04-12"  tinyBalance: {balanceId: 157 acmapCode: "A" quantity: 3123.21}) {
     balanceId
     acmapCode
     quantity
   }
}
```