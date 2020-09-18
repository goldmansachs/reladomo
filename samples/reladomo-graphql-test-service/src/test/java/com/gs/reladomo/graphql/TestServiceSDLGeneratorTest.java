package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.test.MithraTestResource;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class TestServiceSDLGeneratorTest
{
    private MithraTestResource mithraTestResource;

    @Test
    public void schemaExportTest()
    {
        SDLGenerator generator = new SDLGenerator();
        String filename = "TestServiceSDLGeneratorTest-output.graphqls";
        String relativeLocaton = "target/test-classes/" + filename;
        try
        {
            generator.generate(relativeLocaton);
            GraphQLSchema graphQLSchema = SchemaProvider.forResource(filename);
            GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema).build();
            Assert.assertTrue(graphQL != null);
        } catch (Exception e)
        {
            throw new RuntimeException ("failed generate schema in file:///" + new File(relativeLocaton).getAbsolutePath(), e);
        }
    }

    @Before
    public void initReladomo()
    {
        this.mithraTestResource = SampleLoadServlet.initReladomo("TestReladomoRuntimeConfig.xml", "sample-data.txt");
    }

    @After
    public void teardown()
    {
        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;
    }
}