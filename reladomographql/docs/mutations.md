#Mutations
[Reladomo supports CRUD operations](https://goldmansachs.github.io/reladomo-kata/reladomo-tour-docs/tour-guide.html#N402DA) with the objects. 
The operations can be performed via GraphQL `mutation`. For example:
```graphql
mutation {
  course_insert(course: {name: "Thermodynamics"})
  {
    courseId
    name
  }
}
```

result:
```graphql
{
  "data": {
    "course_insert": {
      "courseId": "202",
      "name": "Thermodynamics",
    }
  }
}
```

In cases where the CRUD operations are performed with [milestoned objects](https://goldmansachs.github.io/reladomo-kata/reladomo-tour-docs/tour-guide.html#N408B5), 
it has to follow the additional rule explained in the [temporal milestoning](temporal-milestoning.md). 

Objects with [source attribute](source-attribute.md) need to include source attribute value in the insert statement.
