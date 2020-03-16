# First Day

### Setup
* Obtain the source

* Run Maven `install` Goal

* Setup maven run configuration for jetty:run in graphql-reladomo-test-service module

* Run `jetty:run` maven goal.
    This will start http service with GraphQL endpoint and Reladomo/H2 sample database.
    - The Reladomo model is generated from the [domain models](graphql-reladomo-test-service/src/main/resources/reladomo/models)
    - The [graphql schema](https://graphql.org/learn/schema/) consists of [meta schema](graphql-reladomo-test-service/src/main/resources/meta.graphqls) and generated [domain schema](graphql-reladomo-test-service/src/main/resources/test_schema.graphqls)  

* Now you should be able to run GraphQL queries. The simplest way is by opening [GraphiQL](http://localhost:8080/).

* Or you can install GraphQL Playground [from Electron JS site](https://electronjs.org/apps/graphql-playground)
    - Open the GraphQL Playground. Select "USE ENDPOINT" in "New Workspace".
    - Enter endpoint URL: http://localhost:8080/graphql and click "OPEN".
    
* Let's play with some queries.
    - you also can find all these queries in the [test](graphql-reladomo-test-service/src/test/java/com/reladomo/graphql/SchemaProviderTest.java)


### First Query
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
