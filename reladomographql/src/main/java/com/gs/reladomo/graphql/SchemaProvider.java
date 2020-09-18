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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import graphql.GraphQL;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

public class SchemaProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaProvider.class);
    public static final String META_GRAPHQLS_NAME = "meta.graphqls";

    private SchemaProvider()
    {
    }

    public static GraphQLSchema forResource(final String schemaResourceName)
    {
        final List<String> metaSdl = readResourceAsString(META_GRAPHQLS_NAME);

        if (metaSdl.size() < 3)
        {
            throw new RuntimeException("The " + META_GRAPHQLS_NAME + " is empty or corrupted.");
        }
        final List<String> sdl = readResourceAsString(schemaResourceName);

        final StringBuilder sdlBuilder = new StringBuilder();
        for (final String each : sdl) sdlBuilder.append(each).append('\n');
        for (final String each : metaSdl) sdlBuilder.append(each).append('\n');

        try
        {
            final GraphQLSchema graphQLSchema = new SchemaProvider().buildSchema(sdlBuilder.toString());
            GraphQL.newGraphQL(graphQLSchema).build();

            return graphQLSchema;
        } catch (final Exception e)
        {
            throw new RuntimeException("Failed to read config from " + schemaResourceName, e);
        }
    }

    /**
     * see: https://www.graphql-java.com/documentation/v12/schema/
     */
    private GraphQLSchema buildSchema(final String sdl)
    {
        final TypeDefinitionRegistry typeRegistry = new graphql.schema.idl.SchemaParser().parse(sdl);

        RuntimeWiring.Builder wiringBuilder = RuntimeWiring.newRuntimeWiring();
        wiringBuilder = wiringBuilder.scalar(ExtendedScalars.DateTime).scalar(ExtendedScalars.Date).scalar(ExtendedScalars.Url).scalar(new TimestampScalar());

        for (final MithraRuntimeCacheController each : MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet())
        {
            final String className = each.getClassName();
            final int shortNameStarts = className.lastIndexOf('.') + 1;
            final String name = StringKit.decapitalize(className, shortNameStarts);
            LOGGER.debug("building " + name + " for " + className);

            final String shortClassName = className.substring(shortNameStarts);

            wiringBuilder = wiringBuilder
                    .type(newTypeWiring("Mutation")
                            .dataFetcher(name + "_insert", new ReladomoMutationFetcher(name, each.getClassName(), each.getFinderInstance())))
                    .type(newTypeWiring("Query")
                            .dataFetcher(name + "ById", new ReladomoQueryFetcher(each.getFinderInstance(), false)))
                    .type(newTypeWiring("Query")
                            .dataFetcher(StringKit.englishPluralize(name), new ReladomoQueryFetcher(each.getFinderInstance(), true)))
                    .type(newTypeWiring("Query")
                            .dataFetcher(name + "_aggregate", new AggregateQueryFetcher(each.getFinderInstance())));

            wiringBuilder = buildMithraObjectFetchers(wiringBuilder, each, shortClassName);
        }

        final RuntimeWiring runtimeWiring = wiringBuilder.build();
        final SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
    }

    private RuntimeWiring.Builder buildMithraObjectFetchers(
            RuntimeWiring.Builder wiringBuilder, final MithraRuntimeCacheController runtimeCacheController, final String shortClassName)
    {
        final RelatedFinder finder = runtimeCacheController.getFinderInstance();
        final TypeRuntimeWiring.Builder mithraObjectTypeWiring = newTypeWiring(shortClassName);

        for (final Attribute attr : finder.getPersistentAttributes())
        {
            wiringBuilder = wiringBuilder.type(mithraObjectTypeWiring
                    .dataFetcher(attr.getAttributeName(), new AttributeDataFetcher(attr)));
        }
        if (finder.getAsOfAttributes() != null)
        {
            for (final AsOfAttribute attr : finder.getAsOfAttributes())
            {
                wiringBuilder = wiringBuilder.type(mithraObjectTypeWiring
                        .dataFetcher(attr.getFromAttribute().getAttributeName(), new TimestampAttributeDataFetcher(attr.getFromAttribute())));
                wiringBuilder = wiringBuilder.type(mithraObjectTypeWiring
                        .dataFetcher(attr.getToAttribute().getAttributeName(), new TimestampAttributeDataFetcher(attr.getToAttribute())));
            }
        }

        return wiringBuilder;
    }

    private static List<String> readResourceAsString(final String resourceName)
    {
        try (InputStream in = SchemaProvider.class.getClassLoader().getResourceAsStream(resourceName))
        {
            return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)).lines().collect(Collectors.toList());
        }
        catch (final Exception e)
        {
            throw new RuntimeException("Failed to read resource \"" + resourceName + "\"", e);
        }
    }

}
