/*
 Copyright 2019 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */

package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.test.MithraTestResource;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class SDLGeneratorTest extends GraphqlTestBase
{
    private MithraTestResource mithraTestResource;

    @Test
    public void schemaExportTest()
    {
        SDLGenerator generator = new SDLGenerator();
        String filename = "sample.graphqls";
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
        System.out.println("you are here: " + new File(".").getAbsolutePath());
        this.mithraTestResource = this.initReladomo("ReladomoRuntimeConfig.xml", "test-data.txt");
    }

    @After
    public void teardown()
    {
        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;
    }
}