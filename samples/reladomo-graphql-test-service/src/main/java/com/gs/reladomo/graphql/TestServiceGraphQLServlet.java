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

import com.google.common.base.Strings;
import graphql.schema.GraphQLSchema;
import graphql.servlet.GraphQLHttpServlet;
import graphql.servlet.config.GraphQLConfiguration;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

@WebServlet(name = "HTTP endpoint for GraphQL-Reladomo stack", urlPatterns = "/graphql")
public class TestServiceGraphQLServlet extends GraphQLHttpServlet
{
    private static String schemaResourceName = null;
    private static GraphQLSchema graphQLSchema = null;

    //for Preflight
    @Override
    protected void doOptions(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse)
            throws ServletException, IOException
    {
        setAccessControlHeaders(httpServletRequest, httpServletResponse);
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse)
            throws ServletException, IOException
    {
        setAccessControlHeaders(httpServletRequest, httpServletResponse);
        super.doPost(httpServletRequest, httpServletResponse);
    }

    @Override
    protected void doGet(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse)
            throws ServletException, IOException
    {
        setAccessControlHeaders(httpServletRequest, httpServletResponse);
        super.doGet(httpServletRequest, httpServletResponse);
    }

    private void setAccessControlHeaders(HttpServletRequest httpRequest, HttpServletResponse httpResponse)
    {
        List<String> originUrls = Collections.list(httpRequest.getHeaders("Origin"));
        httpResponse.setHeader("Access-Control-Allow-Origin", String.join(",", originUrls));
        httpResponse.setHeader("Access-Control-Allow-Headers", "Accept,Authorization,Content-Type,If-Match,If-None-Match,Origin,X-Requested-With");
        httpResponse.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS,PUT,PATCH,DELETE");
        httpResponse.setHeader("Access-Control-Allow-Credentials", "true");
    }

    @Override
    protected GraphQLConfiguration getConfiguration()
    {
        String genResource = System.getProperty("generated.graphqls");
        if (genResource != null)
        {
            new SDLGenerator().generate(genResource);
        }

        synchronized (TestServiceGraphQLServlet.class)
        {
            if (graphQLSchema == null) graphQLSchema = SchemaProvider.forResource(schemaResourceName);
        }
        return GraphQLConfiguration.with(graphQLSchema).build();
    }

    public static void setSchemaResourceName(String str)
    {
        schemaResourceName = str;
    }
}