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

import graphql.schema.GraphQLSchema;
import graphql.servlet.GraphQLHttpServlet;
import graphql.servlet.config.GraphQLConfiguration;

import javax.servlet.annotation.WebServlet;

@WebServlet(name = "HTTP endpoint for GraphQL-Reladomo stack", urlPatterns = "/graphql")
public class GraphQLReladomoServlet extends GraphQLHttpServlet
{
    private static String schemaResourceName = null;
    private static GraphQLSchema graphQLSchema = null;

    @Override
    protected GraphQLConfiguration  getConfiguration ()
    {
        String genResource = System.getProperty ("generated.graphqls");
        if (genResource != null)
        {
            new SDLGenerator().generate(genResource);
        }

        synchronized (GraphQLReladomoServlet.class)
        {
            if (graphQLSchema == null) graphQLSchema = SchemaProvider.forResource (schemaResourceName);
        }
        return GraphQLConfiguration.with (graphQLSchema).build ();
    }

    public static void setSchemaResourceName (String str)
    {
        schemaResourceName = str;
    }
}