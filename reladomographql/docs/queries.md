# Queries in GraphQL
If you not done so, setup your environment according to [getting started](getting-started.md).

### Simple Query
The first example allows to query information about a person by its unique personId. 
To do it, enter query text into the left pane. 
```graphql
query {
  personById(personId: 1)
  {
    personId
    firstName
  }
}
```
Click (>) button. You should see the result of execution in the right pane.
Note: the service only returns fields defined in the selection part of the query.

### Relationship (Nested Object Queries)
Now let's try to query objects (Course) and related object (Teacher):
```graphql
{
  courseById(courseId: 101)
  {
    courseId
    name
    teacher {
      firstName
      lastName
    }
  }
}
```
Behind the scenes, the service performed a deep fetch or related object and returned a single result.
You can add other relationships or relationships or relationships as well. The service will determine 
what objects need to be deep fetched and perform necessary joins on the underlying tables. 

### Metadata of the Schema
THe query can include metadata for the queried objects:
```graphql
{
  courseById(courseId: 101)
  {
    __typename
    name
    teacher {
      __typename
      firstName
    }
  }
}
```

### Filters
Some queries need to filter a subset of objects. Reladomo provides a sophisticated filter 
 language (a.k.a. Finder/Operations). These filters can be applied to the GraphQL QL as a part of the query:
When executed against the database, Reladomo will push done the filter to the database level. Only the filtered 
subset is returned back from the database. When executed with Reladomo in-memory cache, the filter
will be applied using Reladomo's off-heap indices.
```graphql
{
  courses (filter: {courseId: {eq: 201} OR: [{name: {startsWith: "Po"}}] })
  {
    courseId
    name
  }
}
```
The query above will return courses with courseId = 201 or name starting with "Po". 
The filter query is defined via operations like "eq", "greaterThen", "startsWith". Take a look at the
[meta schema](graphql-reladomo-test-service/src/main/resources/meta.graphqls) for the complete list of operations
for different data types. These operations can be combined via "AND" and "OR" logical operations.

You can rewrite this filter in [Polish Notation](https://en.wikipedia.org/wiki/Polish_notation).
This allows to create any complex logical expression.
```graphql
{
  courses (filter: {
        OR: [
            {courseId: {eq: 201}},
            { AND: [
                {name: {startsWith: "Łukasiew"}},
                {name: {endsWith: "cz"}}
            ]}
        ]
    })
  {
    courseId
    name
  }
}
```

### Filters with Relationship Traversal
The filters can be applied to the related objects as well as the owner objects. 
In this example the query to return course filters the course based on the teacher's first name.

```graphql
{
  courses (filter: {courseId: {in: [201, 5, 101]} AND: [{teacher: {firstName: {startsWith: "Y"}}}] })
  {
    courseId
    name
    teacher {
      firstName
    }
  }
}
```

### Filters with Numeric Algebra
Earlier you saw examples of filters with an numerical attribute equality. Using special syntax you can also describe
filter based on mathematical equation or inequality.
This query will find all OrderItems where `originalPrice + discountPrice > 13.0`
```graphql
{
     orderItems (filter: {EXPR: {plus: [{originalPrice: {}}, {discountPrice: {}}] greaterThan: 13.0} }) 
    {
        id
        originalPrice
    }
}
```

You can combine multiple math operators in a complex statement.
 At this point the QL supports:
 - binary operators: `plus`, `minus`, `times`, `dividedBy`.
 - unary operator `absoluteValue`
 - and numerical values (with `VAL` statement). The above expression can be
The formula `|originalPrice| + (discountPrice - 13.0) > 0.0` can be represented as following: 
```graphql
{
    orderItems (filter: {EXPR: {plus: [{ absoluteValue: {originalPrice: {}}}, {minus: [{discountPrice: {}}, {VAL: 13.0}] }] greaterThan: 0.0} })
    {
        id
        originalPrice
    }
}
```


### Multiple Queries In One Request
The GraphQL specification allows to combine multiple queries in one request. This is helpful to reduce
the total query time: the service can execute multiple statements in parallel. It also need to run authentication 
and authorization only once.
To execute multiple queries in one request, simply put all requests in one query:
```graphql
{
  courses (filter: {OR: [{courseId: {eq: 101}}, {name: {startsWith: "Po"}}]})
  {
    courseId
    name
  }
  persons
  {
    firstName
  }
}
```

# Tuple-in Queries
Let's say we want to build a filter to find records for people identified by combination of first and last name. 
In earlier example an attribute was filtered by matching to one of the values in a set: `courseId: {in: [201, 5, 101]}`.
This approach only works for a single attribute (`courseId`). For multiple attributes like `firstName` and `lastName`, the
values for each attribute have to be combined together into a 
[tuple](https://goldmansachs.github.io/reladomo-kata/main-kata-presentation/ReladomoKata.xhtml#(97)).

Luckily, Reladomo supports operations on the set of tuples. The query to find people by combination of first and last name
can be implemented as well:

```graphql
query { persons (filter: {
  tupleIn: [
    {firstName: "Aki" lastName: "Abe"},
    {firstName: "Olga" lastName: "Ivanova"},
    {firstName: "John" lastName: "Barkley"},
    {firstName: "John" lastName: "Smith"} 
  ]
})
  {
    firstName
    lastName
  }
}
```

#Order-by and Limited Fetch Size Queries
"order_by:" allows you to define ascending or descending order on an attribute.
"limit:" sets the maximum number of elements retrieved in this operation.
```graphql
{ courses (where: {courseId: {notEq: 11}}
  order_by: {courseId: desc}
  limit: 2) {
    courseId name
  }
}
```
#Filtering of temporal milestoned objects
[see temporal milestoning](temporal-milestoning.md)

#Filtering of objects with source attribute
[see source attribute](source-attribute.md)

##Note
I have used `filter`, `findMany` and `where` to define the filter for the query. All three are interchangeable.