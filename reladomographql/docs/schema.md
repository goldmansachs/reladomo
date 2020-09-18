#Schema
To learn more about GraphQL schema, read [GraphQL Guide](https://developer.github.com/v4/guides/)

For this application, the schema is implemented in two parts:
* [meta schema](../graphql-reladomo/src/main/resources/meta.graphqls)
* [generated schema](../graphql-reladomo-test-service/target/classes/sample.graphqls)

The meta schema defines internal Reladomo constructs like Finders and Attributes. 
The generated schema defines types specific for the Reladomo domain model.
The types are generated based on the [Reladomo runtime config](../graphql-reladomo-test-service/src/main/resources/TestReladomoRuntimeConfig.xml)
and the [Reladomo domain objects definitions](../graphql-reladomo-test-service/src/main/resources/reladomo/models/ReladomoClassList.xml).

The generated schema is produced by executing SchemaGeneratorTest with desired runtime config.
After the schema is generated, if can be modified and stored in a custom location.