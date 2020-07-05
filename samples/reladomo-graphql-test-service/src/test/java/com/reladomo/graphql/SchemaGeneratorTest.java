package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.test.MithraTestResource;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SchemaGeneratorTest {
    private MithraTestResource mithraTestResource;

    @Test
    public void schemaExportTest() {
        SchemaGenerator generator = new SchemaGenerator();
        String filename = "sample.graphqls";
        generator.generate("target/classes/" + filename);
        GraphQLSchema graphQLSchema = SchemaProvider.forResource(filename);
        GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema).build();

    }

    @Before
    public void initReladomo() throws Exception {
        this.mithraTestResource = SampleLoadServlet.initReladomo("TestReladomoRuntimeConfig.xml", "test_data.txt");
    }

    @After
    public void teardown() {
        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;
    }

}