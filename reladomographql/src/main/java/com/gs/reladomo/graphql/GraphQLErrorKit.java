package com.gs.reladomo.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import graphql.GraphQLError;

import java.util.Collection;

public final class GraphQLErrorKit
{
    private static final ObjectWriter mapper = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private GraphQLErrorKit()
    {
    }

    public static String toString(final Collection<GraphQLError> errors)
    {
        final StringBuilder stringBuilder = new StringBuilder();
        for (final GraphQLError each : errors)
        {
            stringBuilder.append(toString(each));
        }

        return stringBuilder.toString();
    }

    public static String toString(final GraphQLError error)
    {
        try
        {
            return mapper.writeValueAsString(error);
        }
        catch (final Exception e)
        {
            return "" + error;
        }
    }
}
