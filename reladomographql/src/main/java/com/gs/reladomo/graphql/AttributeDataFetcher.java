package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

class AttributeDataFetcher<V> implements DataFetcher<V>
{
    private final Attribute<MithraObject, V> attr;

    public AttributeDataFetcher(final Attribute<MithraObject, V> attr)
    {
        this.attr = attr;
    }

    @Override
    public V get(final DataFetchingEnvironment environment) throws Exception
    {
        final Object obj = environment.getSource();
        if (obj instanceof BeanWrapper)
        {
            return (V) ((BeanWrapper) obj).get(attr.getAttributeName());
        } else
        {
            return this.attr.valueOf((MithraObject) obj);
        }
    }
}
