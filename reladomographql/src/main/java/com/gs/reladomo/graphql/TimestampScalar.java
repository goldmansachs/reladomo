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


import graphql.Internal;
import graphql.language.StringValue;
import graphql.schema.*;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

import static graphql.scalars.util.Kit.typeName;

/**
 * Implements Scalar to match the Reladomo's ImmutableTimestamp
 *
 * It is represented in the  RFC-3339 compliant / ISO 8601-UTC format.
 * In addition it supports the milestoning Infinity timestamp. The infinity timestamp is declared in
 * the TimestampAttribute.getAsOfAttributeInfinity() or AsOfAttribute.getInfinityDate()
 */
@Internal
public class TimestampScalar extends GraphQLScalarType
{
    public static final String INFINITY_JSON = "INFINITY";
    public static final Timestamp INFINITY_TIMESTAMP_MARKER = new Timestamp (Long.MAX_VALUE); // intermediate infinity marker "292278994-08-17 02:12:55.807"

    private static final String EXAMPLE = "(i.e. 2019-03-23T14:09:39-00:00)";
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = new ThreadLocal<>();
    private static final ZoneId TIMEZONE = ZoneId.of("UTC");

    private static SimpleDateFormat getDateTimeFormatter()
    {
        SimpleDateFormat format = DATE_FORMATTER.get();
        if (format == null)
        {
            format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            DATE_FORMATTER.set(format);
        }

        return format;
    }

    public TimestampScalar()
    {
        super("Timestamp", "An RFC-3339 compliant Timestamp Scalar " + EXAMPLE, new Coercing<Timestamp, String>()
        {
            @Override
            public String serialize(final Object input) throws CoercingSerializeException
            {
                final OffsetDateTime offsetDateTime;
                if (input instanceof OffsetDateTime)
                {
                    offsetDateTime = (OffsetDateTime) input;
                } else if (input instanceof ZonedDateTime)
                {
                    offsetDateTime = ((ZonedDateTime) input).toOffsetDateTime();
                } else if (input instanceof Timestamp)
                {
                    offsetDateTime = OffsetDateTime.ofInstant(((Timestamp) input).toInstant(), TIMEZONE);
                } else if (input instanceof String)
                {
                    return (String)input;
                }
                else
                {
                    throw new CoercingSerializeException(
                            "Expected something we can convert to 'java.time.OffsetDateTime' but was '" + typeName(input) + "'." + EXAMPLE
                    );
                }
                try
                {
                    return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(offsetDateTime);
                }
                catch (final DateTimeException e)
                {
                    throw new CoercingSerializeException(
                            "Unable to turn TemporalAccessor into OffsetDateTime because of : '" + e.getMessage() + "'."
                    );
                }
            }

            @Override
            public Timestamp parseValue(final Object input) throws CoercingParseValueException
            {
                final Timestamp timestamp;
                if (input instanceof Timestamp)
                {
                    timestamp = (Timestamp) input;
                }
                else if (input instanceof String)
                {
                    final String str = input.toString();
                        timestamp = parseOffsetDateTime(str, CoercingParseValueException::new);
                }
                else
                {
                    throw new CoercingParseValueException("Unexpected type '" + typeName(input) + "'.");
                }
                return timestamp;
            }

            @Override
            public Timestamp parseLiteral(final Object input) throws CoercingParseLiteralException
            {
                if (!(input instanceof StringValue))
                {
                    throw new CoercingParseLiteralException(
                            "Expected AST type 'StringValue' but was '" + typeName(input) + "'."
                    );
                }
                return parseOffsetDateTime(((StringValue) input).getValue(), CoercingParseLiteralException::new);
            }

            private Timestamp parseOffsetDateTime(final String s, final Function<String, RuntimeException> exceptionMaker)
            {
                if (s.equals(INFINITY_JSON))
                {
                    return INFINITY_TIMESTAMP_MARKER;
                }
                else if (s.length() == 10)
                {
                    try
                    {
                        return Timestamp.valueOf(s + " 23:59:00");
                    }
                    catch (final Exception e)
                    {
                        throw exceptionMaker.apply("Invalid ISO8601 date value : '" + s + "'. because of : '" + e.getMessage() + "'");
                    }
                }
                else
                {
                    try
                    {
                        return new Timestamp(getDateTimeFormatter().parse(s).getTime());
                    }
                    catch (final Exception e)
                    {
                        throw exceptionMaker.apply("Invalid RFC3339 value : '" + s + "'. because of : '" + e.getMessage() + "'");
                    }
                }
            }
        });
    }
}
