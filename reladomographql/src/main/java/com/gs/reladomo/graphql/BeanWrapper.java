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

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.util.MutableComparableReference;
import com.gs.fw.common.mithra.util.MutableNumber;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

class BeanWrapper implements Map
{
    private final AggregateData row;
    private final Map<String, Bean> beanMap;

    BeanWrapper(final AggregateData row, final Map<String, Bean> beanMap)
    {
        this.row = row;
        this.beanMap = beanMap;
    }

    protected interface Bean
    {
        Object valueOf(AggregateData aggregateData);
    }

    protected static class StringLeaf implements Bean
    {
        private final int index;

        StringLeaf(final int index)
        {
            this.index = index;
        }

        @Override
        public Object valueOf(final AggregateData aggregateData)
        {
            return ((MutableComparableReference) aggregateData.getValueAt(index)).getAsObject();
        }
    }


    protected static class AggregateNode implements Bean
    {
        private int index;
        private final String attributeName;
        private final List<String> types;

        AggregateNode(final String attributeName, final List<String> types)
        {
            this.attributeName = attributeName;
            this.types = types;
        }

        void setIndex(final int index)
        {
            this.index = index;
        }

        public List<String> getTypes()
        {
            return types;
        }

        public String getAttributeName()
        {
            return attributeName;
        }

        @Override
        public Object valueOf(final AggregateData aggregateData)
        {
            MutableMap bean = Maps.mutable.of();
            for (int i = 0; i < this.types.size(); i++)
            {
                bean = bean.withKeyValue(types.get(i), ((MutableNumber) aggregateData.getValueAt(index + i)).getAsObject());
            }
            return bean;
        }
    }

    protected static class Node implements Bean
    {
        private final Map<String, Bean> beanMap;

        Node(final Map<String, Bean> beanMap)
        {
            this.beanMap = beanMap;
        }

        @Override
        public Object valueOf(final AggregateData row)
        {
            return new BeanWrapper(row, this.beanMap);
        }
    }

    @Override
    public Object get(final Object key)
    {
        final Bean bean = this.beanMap.get(key);
        return bean == null ? null : bean.valueOf(this.row);
    }

    @Override
    public int size()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isEmpty()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean containsKey(final Object key)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean containsValue(final Object value)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Object put(final Object key, final Object value)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Object remove(final Object key)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void putAll(final Map m)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void clear()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Set keySet()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Collection values()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Set<Entry> entrySet()
    {
        throw new RuntimeException("not implemented");
    }
}
