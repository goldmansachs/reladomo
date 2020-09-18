package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.sql.Timestamp;

class TimestampAttributeDataFetcher implements DataFetcher
{
    private final TimestampAttribute attr;

    public TimestampAttributeDataFetcher(final TimestampAttribute attr)
    {
        this.attr = attr;
    }

    @Override
    public Object get(final DataFetchingEnvironment environment) throws Exception
    {
        Timestamp timestamp = this.attr.valueOf(environment.getSource());
        if (timestamp == null)
        {
            if (attr.isInfiniteNull()) return TimestampScalar.INFINITY_JSON;
            return null;
        }
        if (timestamp.equals(attr.getAsOfAttributeInfinity())) return TimestampScalar.INFINITY_JSON;
        return timestamp;
    }
}
